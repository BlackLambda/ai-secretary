/* ------------------------------------------------------------------ */
/*  AI Secretary – Background Service Worker  v1.1                    */
/*  Badge shows assignee TASK (card) count, not individual items.     */
/*  _sw_bust_20260305a_                                               */
/* ------------------------------------------------------------------ */
'use strict';

const ALARM_NAME = 'ai-secretary-refresh';
const DEFAULTS = { serverUrl: 'http://localhost:5000', refreshInterval: 30 };

/* ---------- Settings ---------- */
async function getSettings() {
  return new Promise(resolve => {
    chrome.storage.sync.get(DEFAULTS, resolve);
  });
}

/* ---------- Fetch helpers ---------- */
async function apiFetch(path) {
  const { serverUrl } = await getSettings();
  const base = serverUrl.replace(/\/+$/, '');
  const resp = await fetch(`${base}${path}`, { mode: 'cors' });
  if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
  return resp.json();
}

/* ---------- Role normalisation (same as popup) ---------- */
function normalizeUserRole(raw) {
  if (!raw || typeof raw !== 'string') return undefined;
  const norm = new Set(raw.toLowerCase().replace(/[^a-z]/g, ' ').split(/\s+/));
  if (norm.has('assignee')) return 'assignee';
  if (norm.has('collaborator')) return 'collaborator';
  if (norm.has('observer')) return 'observer';
  return undefined;
}

/* ---------- Badge update ---------- */
async function updateBadge() {
  try {
    const data = await apiFetch('/api/briefing_data');
    const cards = data.cards || [];
    const ops = data.user_ops || {};
    const completed = new Set(ops.completed || []);
    const dismissed = new Set(ops.dismissed || []);

    // Count assignee TASKS (cards with at least one active assignee item)
    let assigneeCount = 0;
    for (const card of cards) {
      const items = card.type === 'Outlook'
        ? [...(card.data?.todos || []), ...(card.data?.recommendations || [])]
        : [...(card.data?.linked_items || []), ...(card.data?.unlinked_items || []), ...(card.data?.conversation?.tasks || [])];

      let hasActiveAssignee = false;
      for (const item of items) {
        const uid = item._ui_id || '';
        if (uid && (completed.has(uid) || dismissed.has(uid))) continue;

        const od = item.original_data || {};
        const role = normalizeUserRole(od.user_role ?? item.user_role);
        if (role === 'assignee') { hasActiveAssignee = true; break; }
      }
      if (hasActiveAssignee) assigneeCount++;
    }

    // Set badge — assignee count only
    const text = assigneeCount > 0 ? String(assigneeCount) : '';
    chrome.action.setBadgeText({ text });
    chrome.action.setBadgeBackgroundColor({ color: assigneeCount > 0 ? '#3498db' : '#888' });
  } catch {
    chrome.action.setBadgeText({ text: '!' });
    chrome.action.setBadgeBackgroundColor({ color: '#e74c3c' });
  }
}

/* ---------- Alarm setup ---------- */
async function setupAlarm() {
  const { refreshInterval } = await getSettings();
  const periodInMinutes = Math.max(0.5, refreshInterval / 60);
  chrome.alarms.create(ALARM_NAME, { periodInMinutes });
}

chrome.alarms.onAlarm.addListener((alarm) => {
  if (alarm.name === ALARM_NAME) {
    updateBadge();
  }
});

/* ---------- Lifecycle ---------- */
chrome.runtime.onInstalled.addListener(() => {
  setupAlarm();
  updateBadge();
});

chrome.runtime.onStartup.addListener(() => {
  setupAlarm();
  updateBadge();
});

// Re-setup alarm when settings change
chrome.storage.onChanged.addListener((changes) => {
  if (changes.refreshInterval) {
    setupAlarm();
  }
});

// Listen for messages from popup or other extension pages
chrome.runtime.onMessage.addListener((msg, _sender, sendResponse) => {
  if (msg?.type === 'updateBadge') {
    updateBadge().then(() => sendResponse({ ok: true })).catch(() => sendResponse({ ok: false }));
    return true; // async response
  }
});
