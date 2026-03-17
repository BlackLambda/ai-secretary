/* ------------------------------------------------------------------ */
/*  AI Secretary – Browser Extension Popup Logic                      */
/*  Aligned with dashboard v2 (StatsBar + sort chips + CardStream)    */
/* ------------------------------------------------------------------ */
'use strict';

/* ---------- Constants / State ---------- */
const DEFAULTS = { serverUrl: 'http://localhost:5000', refreshInterval: 30 };

// Dashboard-style filter state
let filterStatus = 'active';    // 'active' | 'done'
let filterRole   = 'assignee';  // 'assignee' | 'collaborator' | 'observer' | null
let sortBy       = 'priority';  // 'priority' | 'updated' | 'deadline' | 'received' (aligned with dashboard)

let allCards = [];
let userOps = {};
let pipelineState = 'offline';
let pipelineControlBusy = false;

/* ---------- DOM refs ---------- */
const $taskList      = document.getElementById('task-list');
const $loading       = document.getElementById('loading');
const $empty         = document.getElementById('empty');
const $error         = document.getElementById('error');
const $errorMsg      = document.getElementById('error-msg');
const $badge         = document.getElementById('badge');
const $statusBar     = document.getElementById('status-bar');
const $statusText    = document.getElementById('status-text');
const $progressTrack = document.getElementById('progress-track');
const $progressFill  = document.getElementById('progress-fill');
const $progressText  = document.getElementById('progress-text');
const $headerMeta    = document.getElementById('header-meta');
const $btnRefresh    = document.getElementById('btn-refresh');
const $btnRetry      = document.getElementById('btn-retry');
const $btnPipeline   = document.getElementById('btn-pipeline');
const $btnSettings      = document.getElementById('btn-settings');
const $openDash         = document.getElementById('open-dashboard');
const $statsBar         = document.getElementById('stats-bar');
const $sortBar          = document.getElementById('sort-bar');
const $btnStartServer   = document.getElementById('btn-start-server');
const $startServerStatus = document.getElementById('start-server-status');
const $startServerMsg   = document.getElementById('start-server-msg');
const $onboarding       = document.getElementById('onboarding');
const $btnOpenDash      = document.getElementById('btn-open-dashboard');
const $btnOnboardRetry  = document.getElementById('btn-onboard-retry');
const $updateBanner     = document.getElementById('update-banner');
const $updateDetail     = document.getElementById('update-detail');
const $btnOpenDashUpdate = document.getElementById('btn-open-dash-update');
const $btnUpdateDismiss = document.getElementById('btn-update-dismiss');


// Stat counts
const $countAssignee    = document.getElementById('count-assignee');
const $countCollaborator = document.getElementById('count-collaborator');
const $countObserver    = document.getElementById('count-observer');
const $countDone        = document.getElementById('count-done');

/* ---------- Settings helper ---------- */
async function getSettings() {
  return new Promise(resolve => {
    chrome.storage.sync.get(DEFAULTS, resolve);
  });
}

/* ---------- API helpers ---------- */
async function apiFetch(path, opts) {
  const { serverUrl } = await getSettings();
  const base = serverUrl.replace(/\/+$/, '');
  const resp = await fetch(`${base}${path}`, { mode: 'cors', ...opts });
  if (!resp.ok) {
    const err = new Error(`HTTP ${resp.status}`);
    err.status = resp.status;
    throw err;
  }
  return resp.json();
}

/* ---------- Save operation (complete / dismiss) ---------- */
async function saveOperation(type, uiId, active) {
  return apiFetch('/api/save_operation', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ type, id: uiId, active }),
  });
}

/* ---------- Role normalisation (matches dashboard) ---------- */
function normalizeUserRole(raw) {
  if (!raw || typeof raw !== 'string') return undefined;
  const norm = new Set(raw.toLowerCase().replace(/[^a-z]/g, ' ').split(/\s+/));
  if (norm.has('assignee')) return 'assignee';
  if (norm.has('collaborator')) return 'collaborator';
  if (norm.has('observer')) return 'observer';
  return undefined;
}

/* ---------- Priority dot colour (matches dashboard) ---------- */
function priorityColor(p) {
  switch ((p || '').toLowerCase()) {
    case 'high':   return '#e74c3c';
    case 'medium': return '#f39c12';
    case 'low':    return '#27ae60';
    default:       return '#bdc3c7';
  }
}

/* ---------- Data extraction ---------- */

function extractCards(cards, userOps) {
  const completed = new Set(userOps?.completed || []);
  const dismissed = new Set(userOps?.dismissed || []);
  const result = [];

  for (const card of cards) {
    const isOutlook = card.type === 'Outlook';
    const data = card.data;
    let cardTitle, rawItems, eventType, sourceType;

    if (isOutlook) {
      cardTitle = data.event_name || 'Outlook Event';
      rawItems = [...(data.todos || []), ...(data.recommendations || [])];
      eventType = data.event_type || '';
      sourceType = 'outlook';
    } else {
      cardTitle = data.conversation?.chat_name || data.conversation?.summary?.topic || 'Teams Chat';
      rawItems = [...(data.linked_items || []), ...(data.unlinked_items || []), ...(data.conversation?.tasks || [])];
      eventType = '';
      sourceType = 'teams';
    }

    if (!rawItems.length) continue;

    const items = rawItems.map(item => {
      const od = item.original_data || {};
      const uiId = item._ui_id || '';
      const desc = od.description || item.description || od.task || item.task || '(no description)';
      const priority = (od.priority || item.priority || 'Medium');
      const score = item.priority_score ?? 0;
      const role = normalizeUserRole(od.user_role ?? item.user_role);
      const deadline = od.deadline || item.deadline || null;
      const rationale = od.rationale || item.rationale || od.assignment_reason || item.assignment_reason || '';
      const quote = od.original_quote || item.original_quote || '';
      const quoteTime = od.original_quote_timestamp || item.original_quote_timestamp || '';

      let status = 'active';
      if (uiId && completed.has(uiId)) status = 'completed';
      else if (uiId && dismissed.has(uiId)) status = 'dismissed';

      return { uiId, desc, priority, score, role, deadline, rationale, quote, quoteTime, status };
    });

    let maxScore = 0;
    let maxPriority = 'medium';
    for (const it of items) {
      if (it.score > maxScore) { maxScore = it.score; maxPriority = it.priority.toLowerCase(); }
    }

    const activeItems = items.filter(a => a.status === 'active');
    const doneItems = items.filter(a => a.status !== 'active');

    result.push({
      cardTitle,
      sourceType,
      eventType,
      maxScore,
      maxPriority,
      items,
      activeItems,
      doneItems,
      hasActive: activeItems.length > 0,
    });
  }

  result.sort((a, b) => b.maxScore - a.maxScore);
  return result;
}

/* ---------- Compute stats (task = card count) ---------- */

function computeStats(streamCards) {
  let assignee = 0, collaborator = 0, observer = 0, done = 0;

  for (const c of streamCards) {
    // Count done items (individual action items)
    done += c.doneItems.length;

    // For role counts, count cards that have at least one active item with that role
    const roles = new Set(c.activeItems.map(i => i.role).filter(Boolean));
    if (roles.has('assignee')) assignee++;
    if (roles.has('collaborator')) collaborator++;
    if (roles.has('observer') || roles.size === 0) observer++;
  }

  return { assignee, collaborator, observer, done };
}

/* ---------- Filtering (dashboard-style: status + role) ---------- */

function filterCards(streamCards) {
  return streamCards.filter(c => {
    // Status filter
    if (filterStatus === 'active' && !c.hasActive) return false;
    if (filterStatus === 'done' && c.doneItems.length === 0) return false;

    // Role filter (only for active status)
    if (filterStatus === 'active' && filterRole) {
      // Card must have at least one active item matching the role
      if (filterRole === 'observer') {
        // Observer includes items with no role
        return c.activeItems.some(i => i.role === 'observer' || !i.role);
      }
      return c.activeItems.some(i => i.role === filterRole);
    }

    return true;
  });
}

/* ---------- Filter item within a card (for done view) ---------- */
function filterItemsInCard(card) {
  if (filterStatus === 'done') return card.doneItems;

  if (filterRole) {
    return card.activeItems.filter(i => {
      if (filterRole === 'observer') return i.role === 'observer' || !i.role;
      return i.role === filterRole;
    });
  }
  return card.activeItems;
}

/* ---------- Count assignee tasks for badge ---------- */
function countAssigneeTasks(streamCards) {
  let count = 0;
  for (const c of streamCards) {
    if (c.activeItems.some(i => i.role === 'assignee')) count++;
  }
  return count;
}

/* ---------- Sorting (aligned with dashboard) ---------- */

function sortCards(cards) {
  const sorted = [...cards];
  
  if (sortBy === 'priority') {
    // Sort by priority level
    sorted.sort((a, b) => {
      const valA = a.priorityLevel || 999;
      const valB = b.priorityLevel || 999;
      return valA - valB;
    });
  } else if (sortBy === 'updated') {
    // Sort by most recently updated (LastModifiedDateTime or ReceivedDateTime)
    sorted.sort((a, b) => {
      const dateA = new Date(a.card_updated_at || a.card_received_at || 0);
      const dateB = new Date(b.card_updated_at || b.card_received_at || 0);
      return dateB - dateA; // Descending (newest first)
    });
  } else if (sortBy === 'deadline') {
    // Sort by earliest deadline
    sorted.sort((a, b) => {
      const deadlineA = a.nearestDeadline ? new Date(a.nearestDeadline) : new Date('9999-12-31');
      const deadlineB = b.nearestDeadline ? new Date(b.nearestDeadline) : new Date('9999-12-31');
      return deadlineA - deadlineB; // Ascending (earliest first)
    });
  } else if (sortBy === 'received') {
    // Sort by received time
    sorted.sort((a, b) => {
      const dateA = new Date(a.card_received_at || 0);
      const dateB = new Date(b.card_received_at || 0);
      return dateB - dateA; // Descending (newest first)
    });
  }
  
  return sorted;
}

/* ---------- Rendering ---------- */

function renderCards(streamCards) {
  $taskList.querySelectorAll('.stream-card').forEach(el => el.remove());
  $loading.style.display = 'none';
  $error.style.display = 'none';
  $onboarding.style.display = 'none';

  // Update stats
  const stats = computeStats(streamCards);
  $countAssignee.textContent = stats.assignee;
  $countCollaborator.textContent = stats.collaborator;
  $countObserver.textContent = stats.observer;
  $countDone.textContent = stats.done;

  // Badge = assignee task (card) count
  const assigneeCount = countAssigneeTasks(streamCards);
  $badge.textContent = assigneeCount;
  $badge.classList.toggle('has-items', assigneeCount > 0);

  const filtered = filterCards(streamCards);
  const sorted = sortCards(filtered);

  if (!sorted.length) {
    $empty.style.display = '';
    return;
  }
  $empty.style.display = 'none';

  for (const card of sorted) {
    const el = createStreamCard(card);
    $taskList.appendChild(el);
  }
}

function createStreamCard(card) {
  const root = document.createElement('div');
  root.className = 'stream-card';

  // Header
  const header = document.createElement('div');
  header.className = 'stream-card-header';

  const typeIcon = document.createElement('span');
  typeIcon.className = 'stream-card-type';
  typeIcon.textContent = card.sourceType === 'outlook' ? '📧' : '💬';

  const title = document.createElement('span');
  title.className = 'stream-card-title';
  title.textContent = card.cardTitle;

  header.append(typeIcon, title);

  if (card.eventType) {
    const evtTag = document.createElement('span');
    evtTag.className = 'stream-card-event-type';
    evtTag.textContent = card.eventType;
    header.appendChild(evtTag);
  }

  const filteredItems = filterItemsInCard(card);
  const badge = document.createElement('span');
  badge.className = 'stream-card-badge';
  badge.textContent = filteredItems.length;
  header.appendChild(badge);

  root.appendChild(header);

  // Items
  const itemsContainer = document.createElement('div');
  itemsContainer.className = 'stream-card-items';

  const MAX_VISIBLE = 4;
  const visibleItems = filteredItems.slice(0, MAX_VISIBLE);
  const hiddenItems = filteredItems.slice(MAX_VISIBLE);

  for (const item of visibleItems) {
    itemsContainer.appendChild(createStreamItem(item));
  }
  root.appendChild(itemsContainer);

  if (hiddenItems.length) {
    const moreBtn = document.createElement('button');
    moreBtn.className = 'show-more-btn';
    moreBtn.textContent = `+${hiddenItems.length} more`;
    let expanded = false;
    moreBtn.addEventListener('click', (e) => {
      e.stopPropagation();
      expanded = !expanded;
      if (expanded) {
        for (const item of hiddenItems) {
          itemsContainer.appendChild(createStreamItem(item));
        }
        moreBtn.textContent = 'Show less';
      } else {
        while (itemsContainer.children.length > MAX_VISIBLE) {
          itemsContainer.removeChild(itemsContainer.lastChild);
        }
        moreBtn.textContent = `+${hiddenItems.length} more`;
      }
    });
    root.appendChild(moreBtn);
  }

  return root;
}

function createStreamItem(item) {
  const wrapper = document.createElement('div');

  const row = document.createElement('div');
  row.className = `stream-item ${item.status}`;

  const dot = document.createElement('span');
  dot.className = 'item-priority-dot';
  dot.style.backgroundColor = priorityColor(item.priority);
  dot.title = item.priority;

  const body = document.createElement('div');
  body.className = 'item-body';

  const desc = document.createElement('span');
  desc.className = `item-desc ${(item.status === 'completed' || item.status === 'dismissed') ? 'strike' : ''}`;
  desc.textContent = item.desc;
  body.appendChild(desc);

  const meta = document.createElement('div');
  meta.className = 'item-meta';

  if (item.role) {
    const roleTag = document.createElement('span');
    roleTag.className = `item-role role-${item.role}`;
    roleTag.textContent = item.role;
    meta.appendChild(roleTag);
  }

  if (item.quoteTime && !/^(unknown|n\/?a|none|null)$/i.test(String(item.quoteTime).trim())) {
    const qt = document.createElement('span');
    qt.className = 'item-quote-time';
    qt.textContent = formatLocalTime(item.quoteTime);
    meta.appendChild(qt);
  }

  if (item.deadline && !/^(unknown|n\/?a|none|null)$/i.test(String(item.deadline).trim())) {
    const dl = document.createElement('span');
    dl.className = 'item-deadline';
    dl.textContent = 'Due: ' + formatDeadline(item.deadline);
    meta.appendChild(dl);
  }

  const expandHint = document.createElement('span');
  expandHint.className = 'item-expand-hint';
  expandHint.textContent = '▸';
  meta.appendChild(expandHint);

  body.appendChild(meta);

  // Action buttons (complete / dismiss)
  const actions = document.createElement('div');
  actions.className = 'item-actions';

  if (item.status === 'active' && item.uiId) {
    const btnComplete = document.createElement('button');
    btnComplete.className = 'item-action-btn complete-btn';
    btnComplete.title = 'Complete';
    btnComplete.textContent = '✓';
    btnComplete.addEventListener('click', async (e) => {
      e.stopPropagation();
      btnComplete.disabled = true;
      try {
        const resp = await saveOperation('complete', item.uiId, true);
        userOps = resp.ops || userOps;
        rerender();
        chrome.runtime.sendMessage({ type: 'updateBadge' }).catch(() => {});
      } catch (err) { console.error('Complete failed:', err); btnComplete.disabled = false; }
    });

    const btnDismiss = document.createElement('button');
    btnDismiss.className = 'item-action-btn dismiss-btn';
    btnDismiss.title = 'Dismiss';
    btnDismiss.textContent = '✕';
    btnDismiss.addEventListener('click', async (e) => {
      e.stopPropagation();
      btnDismiss.disabled = true;
      try {
        const resp = await saveOperation('dismiss', item.uiId, true);
        userOps = resp.ops || userOps;
        rerender();
        chrome.runtime.sendMessage({ type: 'updateBadge' }).catch(() => {});
      } catch (err) { console.error('Dismiss failed:', err); btnDismiss.disabled = false; }
    });

    actions.append(btnComplete, btnDismiss);
  } else if (item.status !== 'active' && item.uiId) {
    const btnUndo = document.createElement('button');
    btnUndo.className = 'item-action-btn undo-btn';
    btnUndo.title = 'Undo';
    btnUndo.textContent = '↩';
    btnUndo.addEventListener('click', async (e) => {
      e.stopPropagation();
      btnUndo.disabled = true;
      try {
        const undoType = item.status === 'completed' ? 'complete' : 'dismiss';
        const resp = await saveOperation(undoType, item.uiId, false);
        userOps = resp.ops || userOps;
        rerender();
        chrome.runtime.sendMessage({ type: 'updateBadge' }).catch(() => {});
      } catch (err) { console.error('Undo failed:', err); btnUndo.disabled = false; }
    });
    actions.append(btnUndo);
  }

  row.append(dot, body, actions);

  let detailEl = null;
  let isExpanded = false;

  row.addEventListener('click', () => {
    isExpanded = !isExpanded;
    row.classList.toggle('expanded', isExpanded);
    expandHint.textContent = isExpanded ? '▾' : '▸';

    if (isExpanded && !detailEl) {
      detailEl = createDetailPanel(item);
      wrapper.appendChild(detailEl);
    } else if (detailEl) {
      detailEl.style.display = isExpanded ? '' : 'none';
    }
  });

  wrapper.appendChild(row);
  return wrapper;
}

function createDetailPanel(item) {
  const panel = document.createElement('div');
  panel.className = 'item-detail';

  if (item.rationale) {
    panel.appendChild(makeDetailRow('Rationale', item.rationale));
  }
  if (item.quote) {
    const row = document.createElement('div');
    row.className = 'detail-row';
    const label = document.createElement('span');
    label.className = 'detail-label';
    label.textContent = 'Original Quote';
    const quote = document.createElement('blockquote');
    quote.className = 'detail-quote';
    quote.textContent = item.quote;
    row.append(label, quote);
    panel.appendChild(row);
  }
  if (!panel.children.length) {
    panel.appendChild(makeDetailRow('Priority', item.priority));
    if (item.score) panel.appendChild(makeDetailRow('Score', String(item.score)));
  }
  return panel;
}

function makeDetailRow(label, value) {
  const row = document.createElement('div');
  row.className = 'detail-row';
  const l = document.createElement('span');
  l.className = 'detail-label';
  l.textContent = label;
  const v = document.createElement('span');
  v.className = 'detail-value';
  v.textContent = value;
  row.append(l, v);
  return row;
}

/* ---------- Formatting helpers ---------- */

function formatDeadline(raw) {
  try {
    const d = new Date(raw);
    if (isNaN(d.getTime())) return raw;
    const now = new Date();
    const diffMs = d - now;
    const diffDays = Math.ceil(diffMs / 86400000);
    if (diffDays < 0) return 'Overdue';
    if (diffDays === 0) return 'Today';
    if (diffDays === 1) return 'Tomorrow';
    return d.toLocaleDateString(undefined, { month: 'short', day: 'numeric' });
  } catch { return raw; }
}

function formatLocalTime(raw) {
  try {
    const d = new Date(raw);
    if (isNaN(d.getTime())) return raw;
    return d.toLocaleString(undefined, { month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit' });
  } catch { return raw; }
}

function showError(msg) {
  $loading.style.display = 'none';
  $empty.style.display = 'none';
  $error.style.display = '';
  $onboarding.style.display = 'none';
  $errorMsg.textContent = msg || 'Cannot reach AI Secretary server';
}

function showOnboarding() {
  $loading.style.display = 'none';
  $empty.style.display = 'none';
  $error.style.display = 'none';
  $onboarding.style.display = '';
}

/* ---------- Pipeline status UI ---------- */

function updateStatusBar(status) {
  const state = status?.state || 'offline';
  pipelineState = state;
  $statusBar.className = `status-bar status-${state}`;

  const labels = { working: 'Pipeline running…', sleeping: 'Pipeline idle', offline: 'Server offline' };
  $statusText.textContent = status?.message || labels[state] || state;

  // Progress bar (visible when working)
  const progress = status?.progress;
  if (state === 'working' && typeof progress === 'number') {
    $progressTrack.style.display = '';
    $progressFill.style.width = `${progress}%`;
    const stageLabel = status?.current_step_id || '';
    $progressText.textContent = `${progress}%${stageLabel ? ' · ' + stageLabel : ''}`;
  } else {
    $progressTrack.style.display = 'none';
    $progressText.textContent = '';
  }

  // Pipeline button state
  const canStart = state === 'offline' || state === 'sleeping';
  const isSleeping = state === 'sleeping';
  $btnPipeline.disabled = pipelineControlBusy;
  if (pipelineControlBusy) {
    $btnPipeline.className = 'pipeline-btn rerun';
    $btnPipeline.querySelector('.pipeline-btn-icon').textContent = '⏳';
    $btnPipeline.querySelector('.pipeline-btn-label').textContent = '…';
  } else if (canStart) {
    $btnPipeline.className = `pipeline-btn ${isSleeping ? 'rerun' : 'start'}`;
    $btnPipeline.querySelector('.pipeline-btn-icon').textContent = '▶';
    $btnPipeline.querySelector('.pipeline-btn-label').textContent = isSleeping ? 'Re-run' : 'Start';
    $btnPipeline.title = isSleeping ? 'Re-run Pipeline' : 'Start Pipeline';
  } else {
    $btnPipeline.className = 'pipeline-btn stop';
    $btnPipeline.querySelector('.pipeline-btn-icon').textContent = '■';
    $btnPipeline.querySelector('.pipeline-btn-label').textContent = 'Stop';
    $btnPipeline.title = 'Stop Pipeline';
  }

  // Header meta — show last updated
  if (status?.last_updated) {
    try {
      const d = new Date(status.last_updated);
      if (!isNaN(d.getTime())) {
        $headerMeta.textContent = 'Updated ' + d.toLocaleTimeString(undefined, { hour: '2-digit', minute: '2-digit' });
      }
    } catch { /* ignore */ }
  }
}

/* ---------- Stats bar highlighting ---------- */

function updateStatsBarUI() {
  $statsBar.querySelectorAll('.stat-pill').forEach(pill => {
    pill.classList.remove('active', 'role-assignee', 'role-collaborator', 'role-observer', 'role-done');

    const role = pill.dataset.role;
    const status = pill.dataset.status;

    if (status === 'done' && filterStatus === 'done') {
      pill.classList.add('active', 'role-done');
    } else if (role && filterStatus === 'active' && filterRole === role) {
      pill.classList.add('active', `role-${role}`);
    }
  });
}

function updateSortBarUI() {
  $sortBar.querySelectorAll('.sort-chip').forEach(chip => {
    chip.classList.toggle('active', chip.dataset.sort === sortBy);
  });
}

/* ---------- Data loading ---------- */

async function loadData() {
  $btnRefresh.classList.add('spinning');
  try {
    const [briefing, status] = await Promise.all([
      apiFetch('/api/briefing_data'),
      apiFetch('/api/pipeline_status'),
    ]);

    allCards = briefing.cards || [];
    userOps = briefing.user_ops || {};
    const streamCards = extractCards(allCards, userOps);
    renderCards(streamCards);
    updateStatusBar(status);

    // Tell background service worker to refresh toolbar badge immediately
    chrome.runtime.sendMessage({ type: 'updateBadge' }).catch(() => {});

    // Check for updates in the background (non-blocking)
    checkForUpdate();
  } catch (err) {
    console.error('AI Secretary extension: load failed', err);
    if (err.status === 404) {
      // Server is running but no briefing data yet
      // Still try to get pipeline status
      try {
        const status = await apiFetch('/api/pipeline_status');
        updateStatusBar(status);
        // If pipeline is actively working, don't show onboarding — just show a waiting message
        if (status?.state === 'working') {
          $loading.style.display = '';
          $loading.textContent = 'Pipeline is running — waiting for data…';
          $empty.style.display = 'none';
          $error.style.display = 'none';
          $onboarding.style.display = 'none';
        } else {
          showOnboarding();
        }
      } catch {
        updateStatusBar({ state: 'sleeping', message: 'No data yet' });
        showOnboarding();
      }
    } else {
      showError(err.message?.includes('Failed to fetch') ? 'Cannot reach AI Secretary server' : err.message);
      updateStatusBar({ state: 'offline' });
    }
  } finally {
    $btnRefresh.classList.remove('spinning');
  }
}

function rerender() {
  const streamCards = extractCards(allCards, userOps);
  renderCards(streamCards);
}

/* ---------- Pipeline control ---------- */

async function togglePipeline() {
  const canStart = pipelineState === 'offline' || pipelineState === 'sleeping';
  const endpoint = canStart ? '/api/pipeline_start' : '/api/pipeline_stop';

  pipelineControlBusy = true;
  updateStatusBar({ state: pipelineState });

  try {
    await apiFetch(endpoint, { method: 'POST' });
    // Refresh status after a short delay
    setTimeout(async () => {
      try {
        const status = await apiFetch('/api/pipeline_status');
        updateStatusBar(status);
      } catch { /* ignore */ }
      pipelineControlBusy = false;
      updateStatusBar({ state: pipelineState });
    }, 1500);
  } catch (err) {
    console.error('Pipeline toggle failed:', err);
    pipelineControlBusy = false;
    updateStatusBar({ state: pipelineState });
  }
}

/* ---------- Event handlers ---------- */

// Stats bar (Row 1) — role pills
$statsBar.addEventListener('click', (e) => {
  const pill = e.target.closest('.stat-pill');
  if (!pill) return;

  if (pill.dataset.status === 'done') {
    filterStatus = 'done';
    filterRole = null;
  } else if (pill.dataset.role) {
    filterStatus = 'active';
    filterRole = pill.dataset.role;
  }

  updateStatsBarUI();
  rerender();
});

// Sort bar (Row 2) — sorting options (aligned with dashboard)
$sortBar.addEventListener('click', (e) => {
  const chip = e.target.closest('.sort-chip');
  if (!chip) return;
  sortBy = chip.dataset.sort;
  updateSortBarUI();
  rerender();
});

// Pipeline button
$btnPipeline.addEventListener('click', () => togglePipeline());

// Start Server button (when offline)
$btnStartServer?.addEventListener('click', () => startServer());

// Onboarding: Open Dashboard button
$btnOpenDash?.addEventListener('click', async (e) => {
  e.preventDefault();
  const { serverUrl } = await getSettings();
  chrome.tabs.create({ url: serverUrl });
});

// Onboarding: Refresh button
$btnOnboardRetry?.addEventListener('click', () => loadData());

// Update banner: Open Dashboard
$btnOpenDashUpdate?.addEventListener('click', async () => {
  const { serverUrl } = await getSettings();
  chrome.tabs.create({ url: serverUrl });
});

// Update banner: Dismiss
$btnUpdateDismiss?.addEventListener('click', () => {
  if ($updateBanner) $updateBanner.style.display = 'none';
  // Remember dismissal for this session
  _updateDismissed = true;
});

// Refresh
$btnRefresh.addEventListener('click', () => loadData());
$btnRetry?.addEventListener('click', () => loadData());

// Settings
$btnSettings.addEventListener('click', () => chrome.runtime.openOptionsPage());

// Open dashboard
$openDash.addEventListener('click', async (e) => {
  e.preventDefault();
  const { serverUrl } = await getSettings();
  chrome.tabs.create({ url: serverUrl });
});

/* ---------- Git update check ---------- */

let _updateDismissed = false;

async function checkForUpdate() {
  if (_updateDismissed) return;
  try {
    const data = await apiFetch('/api/check_update');
    if ((data.has_update || data.server_stale) && $updateBanner) {
      let detail;
      if (data.has_update) {
        detail = data.message
          ? `${data.current} → ${data.latest}: ${data.message}`
          : `${data.current} → ${data.latest}`;
      } else {
        detail = `Running ${data.server_commit || '?'}, current code is ${data.current || '?'}`;
      }
      if ($updateDetail) $updateDetail.textContent = detail;
      $updateBanner.style.display = '';
    }
  } catch {
    // Server unreachable or endpoint missing — skip silently
  }
}

/* ---------- Start Server (protocol handler) ---------- */

let _startServerPollTimer = null;

function startServer() {
  // Launch the ai-secretary:// protocol to trigger server_launcher.bat
  try {
    const frame = document.createElement('iframe');
    frame.style.display = 'none';
    frame.src = 'ai-secretary://start';
    document.body.appendChild(frame);
    setTimeout(() => frame.remove(), 3000);
  } catch {
    // Fallback: open in new tab
    chrome.tabs.create({ url: 'ai-secretary://start' });
  }

  // Show "Starting server…" status
  if ($btnStartServer) $btnStartServer.disabled = true;
  if ($startServerStatus) $startServerStatus.style.display = '';
  if ($startServerMsg) $startServerMsg.textContent = 'Starting server…';

  // Poll for server to come online
  let attempts = 0;
  const maxAttempts = 20; // ~40 seconds
  clearInterval(_startServerPollTimer);
  _startServerPollTimer = setInterval(async () => {
    attempts++;
    if ($startServerMsg) {
      $startServerMsg.textContent = `Waiting for server… (${attempts * 2}s)`;
    }
    try {
      const { serverUrl } = await getSettings();
      const resp = await fetch(`${serverUrl.replace(/\/+$/, '')}/api/pipeline_status`, {
        mode: 'cors', signal: AbortSignal.timeout(2000)
      });
      if (resp.ok) {
        // Server is up!
        clearInterval(_startServerPollTimer);
        if ($startServerMsg) $startServerMsg.textContent = 'Server started!';
        setTimeout(() => {
          if ($startServerStatus) $startServerStatus.style.display = 'none';
          if ($btnStartServer) $btnStartServer.disabled = false;
          loadData();
        }, 800);
        return;
      }
    } catch { /* server not ready yet */ }
    if (attempts >= maxAttempts) {
      clearInterval(_startServerPollTimer);
      if ($startServerMsg) $startServerMsg.textContent = 'Server did not respond. Try Retry.';
      if ($btnStartServer) $btnStartServer.disabled = false;
    }
  }, 2000);
}

/* ---------- Init ---------- */
updateStatsBarUI();
updateSortBarUI();
loadData();
