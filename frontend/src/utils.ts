import { Priority, Card, PriorityLevelDefinition } from './types';

function parseDeadlineMs(deadline: unknown): number | null {
  if (!deadline) return null;
  const ms = new Date(String(deadline)).getTime();
  return Number.isNaN(ms) ? null : ms;
}

function getCardEarliestActiveDeadlineMs(
  card: Card,
  completedIds: Set<string>,
  dismissedIds: Set<string>,
  opts?: { watchItemsLower?: Set<string>; taskTodoScoreCutoff?: number }
): number | null {
  let earliest: number | null = null;

  const isBelowCutoff = (item: any) => {
    const cutoff = opts?.taskTodoScoreCutoff;
    if (typeof cutoff !== 'number' || !Number.isFinite(cutoff) || cutoff <= 0) return false;
    const score = item?.original_data?.priority_score ?? item?.priority_score;
    if (typeof score !== 'number' || Number.isNaN(score)) return false;
    return score < cutoff;
  };

  const considerItem = (item: any) => {
    if (item?._ui_id && (completedIds.has(item._ui_id) || dismissedIds.has(item._ui_id))) return;
    if (isBelowCutoff(item)) return;
    const raw = item?.original_data?.deadline ?? item?.deadline;
    const ms = parseDeadlineMs(raw);
    if (ms === null) return;
    if (earliest === null || ms < earliest) earliest = ms;
  };

  if (card.type === 'Outlook') {
    const event = card.data as any;
    const eventType = String(event.event_type || '').toLowerCase();
    const isLikedEvent = !!eventType && !!opts?.watchItemsLower?.has(eventType);

    (event.todos || []).forEach((t: any) => considerItem(t));
    (event.recommendations || []).forEach((r: any) => {
      const userRole = r?.original_data?.user_role || r?.user_role;
      if (userRole !== 'observer' || isLikedEvent) considerItem(r);
    });
  } else {
    const teamsData = card.data as any;
    const allItems = [...(teamsData.linked_items || []), ...(teamsData.unlinked_items || [])];
    allItems.forEach((item: any) => {
      // Teams: both Task and Action items should influence sorting.
      if (item?.type === 'Task' || item?.type === 'Action') considerItem(item);
    });
  }

  return earliest;
}

export function formatTimestamp(timestamp: string | undefined): string {
  if (!timestamp || timestamp === 'Unknown') return 'Unknown';
  
  try {
    const date = new Date(timestamp);
    return date.toLocaleString('en-US', {
      month: 'short',
      day: 'numeric',
      hour: '2-digit',
      minute: '2-digit',
    });
  } catch {
    return timestamp;
  }
}

export type UiTimeKind = 'quote' | 'detect' | 'update';

export function formatTimestampMaybe(raw: unknown): string | null {
  if (raw === undefined || raw === null) return null;
  const s = String(raw).trim();
  if (!s) return null;
  if (s.toLowerCase() === 'unknown') return null;

  // If it looks like an ISO-ish timestamp, normalize via formatTimestamp.
  // Otherwise treat it as already-human-readable.
  if (/^\d{4}-\d{2}-\d{2}/.test(s) || s.includes('T') || s.endsWith('Z')) {
    const ms = new Date(s).getTime();
    if (!Number.isNaN(ms)) return formatTimestamp(new Date(ms).toISOString());
  }

  return s;
}

export function formatTimeChipText(
  kind: UiTimeKind,
  raw: unknown,
  opts?: { showLabel?: boolean },
): string | null {
  const v = formatTimestampMaybe(raw);
  if (!v) return null;
  const showLabel = opts?.showLabel ?? true;
  if (!showLabel) return v;
  const label = kind === 'quote' ? 'Quote' : kind === 'detect' ? 'Detected' : 'Updated';
  return `${label}: ${v}`;
}

export function getLatestCardQuoteTimestampMs(card: Card): number | null {
  let latest: number | null = null;

  const considerTs = (ts: unknown) => {
    if (!ts) return;
    if (Array.isArray(ts)) {
      ts.forEach(considerTs);
      return;
    }
    const s = typeof ts === 'string' ? ts.trim() : String(ts).trim();
    if (!s) return;
    const ms = new Date(s).getTime();
    if (Number.isNaN(ms)) return;
    if (latest === null || ms > latest) latest = ms;
  };

  const considerItem = (item: any) => {
    if (!item || typeof item !== 'object') return;
    considerTs(item.original_quote_timestamp);
    considerTs(item.original_data?.original_quote_timestamp);
  };

  if (card.type === 'Outlook') {
    const event: any = card.data as any;
    (event.todos || []).forEach(considerItem);
    (event.recommendations || []).forEach(considerItem);
  } else if (card.type === 'Teams') {
    const teamsData: any = card.data as any;
    (teamsData.linked_items || []).forEach(considerItem);
    (teamsData.unlinked_items || []).forEach(considerItem);
    (teamsData?.conversation?.tasks || []).forEach(considerItem);
  }

  return latest;
}

export function sortCardsByQuoteTimeLatest(cards: Card[]): Card[] {
  // Latest quote time: most recent first.
  // Cards without any quote timestamps go last.
  return [...cards].sort((a, b) => {
    const aMs = getLatestCardQuoteTimestampMs(a);
    const bMs = getLatestCardQuoteTimestampMs(b);

    if (aMs === null && bMs !== null) return 1;
    if (aMs !== null && bMs === null) return -1;
    if (aMs !== null && bMs !== null && aMs !== bMs) return bMs - aMs;

    // Tie-breaker: keep newest updated cards first.
    return getLatestCardTimestamp(b) - getLatestCardTimestamp(a);
  });
}

export function getPriorityValue(priority: Priority | undefined): number {
  switch (priority) {
    case 'High':
      return 0;
    case 'Medium':
      return 1;
    case 'Low':
      return 2;
    default:
      return 1;
  }
}

export function generateUiId(prefix: string, index: number): string {
  return `${prefix}-${index}`;
}

function getCardMaxActiveTaskPriorityScore(
  card: Card,
  completedIds: Set<string>,
  dismissedIds: Set<string>,
  opts?: { watchItemsLower?: Set<string>; taskTodoScoreCutoff?: number }
): number | null {
  let best: number | null = null;

  // Intentionally ignore completed/dismissed state for card-level ranking.
  // (This keeps task/card rank stable even after marking items done/dismissed.)
  void completedIds;
  void dismissedIds;

  const isBelowCutoff = (item: any) => {
    const cutoff = opts?.taskTodoScoreCutoff;
    if (typeof cutoff !== 'number' || !Number.isFinite(cutoff) || cutoff <= 0) return false;
    const score = item?.original_data?.priority_score ?? item?.priority_score;
    if (typeof score !== 'number' || Number.isNaN(score)) return false;
    return score < cutoff;
  };

  const considerItem = (item: any) => {
    if (isBelowCutoff(item)) return;
    const score = item?.original_data?.priority_score ?? item?.priority_score;
    if (typeof score !== 'number' || Number.isNaN(score)) return;
    if (best === null || score > best) best = score;
  };

  if (card.type === 'Outlook') {
    const event = card.data as any;
    const eventType = String(event.event_type || '').toLowerCase();
    const isLikedEvent = !!eventType && !!opts?.watchItemsLower?.has(eventType);

    (event.todos || []).forEach((t: any) => considerItem(t));
    (event.recommendations || []).forEach((r: any) => {
      const userRole = r?.original_data?.user_role || r?.user_role;
      if (userRole !== 'observer' || isLikedEvent) considerItem(r);
    });
  } else if (card.type === 'Teams') {
    const teamsData = card.data as any;
    const convTasks = Array.isArray(teamsData?.conversation?.tasks) ? teamsData.conversation.tasks : [];
    const allItems = [...(teamsData.linked_items || []), ...(teamsData.unlinked_items || []), ...convTasks];
    allItems.forEach((item: any) => {
      // Teams: both Task and Action items should influence sorting.
      if (item?.type === 'Task' || item?.type === 'Action') considerItem(item);
    });
  }

  return best;
}

function normalizePriority(priority: unknown): Priority | undefined {
  if (priority === 'High' || priority === 'Medium' || priority === 'Low') return priority;
  const p = String(priority || '').toLowerCase();
  if (p === 'high') return 'High';
  if (p === 'medium') return 'Medium';
  if (p === 'low') return 'Low';
  return undefined;
}

export function priorityFromScoreSystem(
  score: number,
  priorityLevels?: PriorityLevelDefinition[] | null,
): Priority {
  if (!Array.isArray(priorityLevels) || priorityLevels.length === 0) {
    // Conservative fallback if the score system hasn't loaded.
    if (score >= 12) return 'High';
    if (score >= 6) return 'Medium';
    return 'Low';
  }

  const rows = priorityLevels
    .map((p) => ({ label: normalizePriority(p?.label), min: Number((p as any)?.min_score) }))
    .filter((p): p is { label: Priority; min: number } => !!p.label && Number.isFinite(p.min))
    .sort((a, b) => b.min - a.min);

  for (const r of rows) {
    if (score >= r.min) return r.label;
  }
  return 'Low';
}

export function extractPriorityLevelsFromScoringSystem(rubric: any): PriorityLevelDefinition[] | null {
  const pls = rubric?.priority_levels;
  if (!Array.isArray(pls)) return null;

  const out: PriorityLevelDefinition[] = [];
  for (const p of pls) {
    const label = normalizePriority(p?.label);
    const min = Number(p?.min_score);
    if (!label) continue;
    if (!Number.isFinite(min)) continue;
    out.push({ label, min_score: min });
  }

  return out.length ? out : null;
}

export function deriveCardPriority(
  card: Card,
  completedIds: Set<string>,
  dismissedIds: Set<string>,
  opts?: {
    watchItemsLower?: Set<string>;
    taskTodoScoreCutoff?: number;
    priorityLevels?: PriorityLevelDefinition[] | null;
  }
): Priority {
  // Prefer score-based priority so UI labels match numeric scores (e.g. sidebar shows High 14).
  const maxScore = getCardMaxActiveTaskPriorityScore(card, completedIds, dismissedIds, opts);
  if (typeof maxScore === 'number' && !Number.isNaN(maxScore)) {
    return priorityFromScoreSystem(maxScore, opts?.priorityLevels);
  }

  // No active actionable items found; fall back to whatever the card/event provides.
  const fallbackFromCard = normalizePriority(card.priority);
  if (fallbackFromCard) return fallbackFromCard;
  if (card.type === 'Outlook') {
    const event = card.data as any;
    const fallbackFromEvent = normalizePriority(event?.priority_level);
    if (fallbackFromEvent) return fallbackFromEvent;
  }
  return 'Medium';
}

export function getStableCardSortKey(card: Card): string {
  const sanitize = (raw: unknown) => {
    const s = String(raw ?? '').trim();
    if (!s) return 'unknown';
    return s
      .replace(/[^a-zA-Z0-9_-]/g, '-')
      .replace(/-+/g, '-')
      .replace(/^-+|-+$/g, '')
      .slice(0, 140);
  };

  const pick = (obj: unknown, keys: string[]) => {
    if (!obj || typeof obj !== 'object') return undefined;
    const rec = obj as Record<string, unknown>;
    for (const k of keys) {
      const v = rec[k];
      if (v !== undefined && v !== null) return v;
    }
    return undefined;
  };

  if (card.type === 'Outlook') {
    const event = card.data as any;
    const key = pick(event, ['event_id', 'eventId', 'id', 'event_name']);
    return `outlook:${sanitize(key)}`;
  }

  const teams = card.data as any;
  const conv = teams?.conversation;
  const key = pick(conv, ['conversation_id', 'chat_id', 'chatId', 'chat_name']);
  return `teams:${sanitize(key)}`;
}

export function sortCardsByPriority(
  cards: Card[],
  completedIds: Set<string>,
  dismissedIds: Set<string>,
  opts?: {
    watchItemsLower?: Set<string>;
    taskTodoScoreCutoff?: number;
    priorityLevels?: PriorityLevelDefinition[] | null;
  }
): Card[] {
  return [...cards].sort((a, b) => {
    // Primary: highest priority_score among this card's active tasks.
    const maxA = getCardMaxActiveTaskPriorityScore(a, completedIds, dismissedIds, opts);
    const maxB = getCardMaxActiveTaskPriorityScore(b, completedIds, dismissedIds, opts);
    if (maxA !== null || maxB !== null) {
      if (maxA === null) return 1;
      if (maxB === null) return -1;
      if (maxA !== maxB) return maxB - maxA; // Descending
    }

    // Fallback: derive priority (score-system-based if available, else from card/event strings).
    const pa = getPriorityValue(deriveCardPriority(a, completedIds, dismissedIds, opts));
    const pb = getPriorityValue(deriveCardPriority(b, completedIds, dismissedIds, opts));
    if (pa !== pb) return pa - pb;

    // Final tie-breaker: most recent first.
    // Use a deterministic stable key so ranks don't reshuffle when last_updated changes.
    return getStableCardSortKey(a).localeCompare(getStableCardSortKey(b));
  });
}

export function getLatestCardTimestamp(card: Card): number {
  let latest = 0;
  const check = (ts?: string) => {
    if (ts && ts !== 'Unknown') {
      const t = new Date(ts).getTime();
      if (!isNaN(t) && t > latest) latest = t;
    }
  };

  if (card.type === 'Outlook') {
    const event = card.data as any;
    
    // Check todos (tasks) timestamps
    (event.todos || []).forEach((t: any) => {
      check(t.last_updated || t.original_data?.last_updated);
    });
    
    // Check recommendations timestamps
    (event.recommendations || []).forEach((r: any) => {
      check(r.last_updated || r.original_data?.last_updated);
    });
    
    // Fallback to event-level timestamp if no action timestamps found
    if (latest === 0) {
      check(event.last_updated);
    }
  } else if (card.type === 'Teams') {
    const teamsData = card.data as any;
    
    // Check all items - tasks and recommendations
    [...(teamsData.linked_items || []), ...(teamsData.unlinked_items || [])].forEach((i: any) => {
      check(i.last_updated || i.original_data?.last_updated);
    });
    
    // Fallback to conversation-level timestamp if no action timestamps found
    if (latest === 0) {
      check(teamsData.last_updated);
    }
  }
  return latest;
}

export function sortCardsByDate(
  cards: Card[],
  completedIds?: Set<string>,
  dismissedIds?: Set<string>,
  opts?: { watchItemsLower?: Set<string>; taskTodoScoreCutoff?: number }
): Card[] {
  // Last update: most recent first. If equal, higher priority first.
  return [...cards].sort((a, b) => {
    const byTs = getLatestCardTimestamp(b) - getLatestCardTimestamp(a);
    if (byTs !== 0) return byTs;

    // Secondary: highest active priority_score first.
    if (completedIds && dismissedIds) {
      const maxA = getCardMaxActiveTaskPriorityScore(a, completedIds, dismissedIds, opts);
      const maxB = getCardMaxActiveTaskPriorityScore(b, completedIds, dismissedIds, opts);
      if (maxA !== null || maxB !== null) {
        if (maxA === null) return 1;
        if (maxB === null) return -1;
        if (maxA !== maxB) return maxB - maxA;
      }
    }

    const aPriority = completedIds && dismissedIds
      ? getPriorityValue(deriveCardPriority(a, completedIds, dismissedIds, opts))
      : getPriorityValue(normalizePriority(a.priority) || 'Medium');
    const bPriority = completedIds && dismissedIds
      ? getPriorityValue(deriveCardPriority(b, completedIds, dismissedIds, opts))
      : getPriorityValue(normalizePriority(b.priority) || 'Medium');

    return aPriority - bPriority;
  });
}

export function sortCardsByDeadlineUrgency(
  cards: Card[],
  completedIds: Set<string>,
  dismissedIds: Set<string>,
  opts?: { watchItemsLower?: Set<string>; taskTodoScoreCutoff?: number }
): Card[] {
  return [...cards].sort((a, b) => {
    const aDeadline = getCardEarliestActiveDeadlineMs(a, completedIds, dismissedIds, opts);
    const bDeadline = getCardEarliestActiveDeadlineMs(b, completedIds, dismissedIds, opts);

    // Primary: soonest deadline first (past due naturally sorts first).
    // Items with unknown deadlines go last.
    if (aDeadline === null && bDeadline !== null) return 1;
    if (aDeadline !== null && bDeadline === null) return -1;
    if (aDeadline !== null && bDeadline !== null && aDeadline !== bDeadline) return aDeadline - bDeadline;

    // For same urgency bucket: highest active priority_score first.
    const maxA = getCardMaxActiveTaskPriorityScore(a, completedIds, dismissedIds, opts);
    const maxB = getCardMaxActiveTaskPriorityScore(b, completedIds, dismissedIds, opts);
    if (maxA !== null || maxB !== null) {
      if (maxA === null) return 1;
      if (maxB === null) return -1;
      if (maxA !== maxB) return maxB - maxA;
    }

    // Fallback: derived High/Medium/Low bucket.
    const aPriority = getPriorityValue(deriveCardPriority(a, completedIds, dismissedIds, opts));
    const bPriority = getPriorityValue(deriveCardPriority(b, completedIds, dismissedIds, opts));
    if (aPriority !== bPriority) return aPriority - bPriority;
    // Tie-breaker: keep most recent first.
    return getLatestCardTimestamp(b) - getLatestCardTimestamp(a);
  });
}
