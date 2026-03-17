/**
 * CardStream — Full-width card list for the v2 UI (replaces SideNav + main panel).
 *
 * Renders cards as a single scrollable stream with inline task items.
 * Clicking an item expands it to show detail (rationale, quote, assignees,
 * open-in-Outlook / open-in-Teams button).
 */
import React, { useMemo, useState } from 'react';
import { Card, DashboardFilter, PriorityLevelDefinition, UserRole, ActionItem, BriefingData, OutlookEvent, TeamsItem } from '../types';
import { getStableCardSortKey } from '../utils';
import './CardStream.css';

interface Props {
  cards: Card[];
  completedIds: Set<string>;
  dismissedIds: Set<string>;
  dashboardFilter: DashboardFilter;
  priorityLevels?: PriorityLevelDefinition[] | null;
  briefingData?: BriefingData | null;
  bannedTypes?: Set<string>;
  onComplete: (itemId: string) => void;
  onDismiss: (itemId: string) => void;
}

/* ------------------------------------------------------------------ */
/*  Helpers                                                            */
/* ------------------------------------------------------------------ */

const normalizeUserRole = (raw: unknown): UserRole | undefined => {
  const norm = new Set(
    String(raw ?? '').split(/[^a-zA-Z]+/).map((s) => s.trim().toLowerCase()).filter(Boolean),
  );
  if (norm.has('assignee')) return 'assignee';
  if (norm.has('collaborator')) return 'collaborator';
  if (norm.has('observer')) return 'observer';
  return undefined;
};

const getCardTitle = (card: Card): string => {
  if (card.type === 'Outlook') {
    const ev: any = card.data;
    return ev?.event_name || 'Outlook Event';
  }
  const t: any = card.data;
  return t?.conversation?.chat_name || t?.conversation?.summary?.topic || 'Teams Chat';
};

const getCardItems = (card: Card): ActionItem[] => {
  if (card.type === 'Outlook') {
    const ev: any = card.data;
    return [...(ev?.todos || []), ...(ev?.recommendations || [])];
  }
  const t: any = card.data;
  return [...(t?.linked_items || []), ...(t?.unlinked_items || []), ...(t?.conversation?.tasks || [])];
};

const priorityDot = (priority: string) => {
  const p = (priority || 'Medium').toLowerCase();
  if (p === 'high') return '#e74c3c';
  if (p === 'low') return '#95a5a6';
  return '#f39c12';
};

/** Format a timestamp string to local time (e.g. "Feb 28, 3:45 PM"). */
const formatLocalTime = (raw: string): string => {
  try {
    const d = new Date(raw);
    if (isNaN(d.getTime())) return raw;
    return d.toLocaleString(undefined, { month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit', hour12: false });
  } catch {
    return raw;
  }
};

/** Build an Outlook Web link from an OutlookEvent or its related threads. */
const getOutlookWebLink = (event: OutlookEvent, threadsLookup?: Record<string, any>): string | null => {
  let link = event.web_link || event.weblink || null;
  if (!link && (event as any).related_thread_ids && threadsLookup) {
    for (const tid of (event as any).related_thread_ids) {
      const thread = threadsLookup[tid];
      if (thread?.messages?.[0]?.WebLink) {
        link = thread.messages[0].WebLink;
        break;
      }
    }
  }
  return link || null;
};

/** Build a Teams deep link from a TeamsItem conversation. */
const getTeamsDeepLink = (teams: TeamsItem): string | null => {
  const convId = teams.conversation?.conversation_id;
  const chatName = teams.conversation?.chat_name;
  if (convId) return `https://teams.microsoft.com/l/chat/${convId}`;
  if (chatName) return `https://teams.microsoft.com/l/chat/0/0?users=&topicName=${encodeURIComponent(chatName)}&message=`;
  return null;
};

/* ------------------------------------------------------------------ */
/*  Component                                                          */
/* ------------------------------------------------------------------ */

export const CardStream: React.FC<Props> = ({
  cards,
  completedIds,
  dismissedIds,
  dashboardFilter,
  priorityLevels: _priorityLevels,
  briefingData,
  bannedTypes,
  onComplete,
  onDismiss,
}) => {

  const [expandedItemId, setExpandedItemId] = useState<string | null>(null);

  // Filter items by dashboard selection (status + role)
  const filterItem = (item: ActionItem): boolean => {
    const uid = (item as any)._ui_id || '';
    const isCompleted = completedIds.has(uid);
    const isDismissed = dismissedIds.has(uid);
    const isActive = !isCompleted && !isDismissed;
    const isDone = isCompleted || isDismissed;

    if (dashboardFilter.status === 'active' && !isActive) return false;
    if (dashboardFilter.status === 'done' && !isDone) return false;

    if (dashboardFilter.status === 'active' && dashboardFilter.role) {
      const role = normalizeUserRole((item as any).original_data?.user_role ?? item.user_role);
      if (dashboardFilter.role === 'observer') {
        return role === 'observer' || role === undefined;
      }
      if (role !== dashboardFilter.role) return false;
    }

    return true;
  };

  // Build filtered card list
  const visibleCards = useMemo(() => {
    const result: { card: Card; items: ActionItem[] }[] = [];
    for (const card of cards) {
      // Skip cards whose event_type is banned
      if (bannedTypes && bannedTypes.size > 0 && card.type === 'Outlook') {
        const et = (card.data as any)?.event_type;
        if (et && typeof et === 'string' && bannedTypes.has(et)) continue;
      }
      const items = getCardItems(card).filter(filterItem);
      if (items.length > 0) {
        result.push({ card, items });
      }
    }
    return result;
  }, [cards, completedIds, dismissedIds, dashboardFilter, bannedTypes]);

  if (visibleCards.length === 0) {
    const noDataAtAll = cards.length === 0;
    return (
      <div className="v2-card-stream-empty">
        <p>No items to show.</p>
        {noDataAtAll ? (
          <p className="v2-card-stream-empty-hint">
            The first fetch can take <strong>15–30 minutes</strong> depending on your data size. Hang tight — results will appear here once the pipeline finishes.
          </p>
        ) : dashboardFilter.status === 'active' && (
          <p className="v2-card-stream-empty-hint">Try switching to a different role filter, or waiting for the pipeline to finish.</p>
        )}
      </div>
    );
  }

  return (
    <div className="v2-card-stream">
      {visibleCards.map(({ card, items }, ci) => {
        const title = getCardTitle(card);
        const isOutlook = card.type === 'Outlook';
        const outlookEvent = isOutlook ? (card.data as OutlookEvent) : null;
        const teamsItem = !isOutlook ? (card.data as TeamsItem) : null;

        // Deep link for the card-level "open in" button
        const cardDeepLink = isOutlook
          ? getOutlookWebLink(outlookEvent!, briefingData?.outlook_threads_lookup)
          : getTeamsDeepLink(teamsItem!);

        const eventType = isOutlook ? (outlookEvent?.event_type || '') : '';

        return (
          <div key={getStableCardSortKey(card)} className="v2-stream-card">
            <div className="v2-stream-card-header">
              <span className={`v2-stream-card-type ${card.type.toLowerCase()}`}>
                {isOutlook ? '📧' : '💬'}
              </span>
              <span className="v2-stream-card-title">{title}</span>
              {eventType && <span className="v2-stream-card-event-type">{eventType}</span>}
              <span className="v2-stream-card-badge">{items.length}</span>
              {cardDeepLink && (
                <a
                  href={cardDeepLink}
                  target="_blank"
                  rel="noopener noreferrer"
                  className="v2-stream-card-open"
                  title={isOutlook ? 'Open in Outlook' : 'Open in Teams'}
                  onClick={(e) => e.stopPropagation()}
                >
                  {isOutlook ? '📧 Open' : '💬 Open'}
                </a>
              )}
            </div>

            <div className="v2-stream-card-items">
              {items.map((item, ii) => {
                const uid = (item as any)._ui_id || `item-${ci}-${ii}`;
                const isCompleted = completedIds.has(uid);
                const isDismissed = dismissedIds.has(uid);
                const description = item.description || item.task || (item as any).original_data?.task || (item as any).original_data?.description || 'Untitled';
                const priority = (item as any).original_data?.priority || item.priority || 'Medium';
                const deadline = (item as any).original_data?.deadline || item.deadline;
                const role = normalizeUserRole((item as any).original_data?.user_role ?? item.user_role);
                const isExpanded = expandedItemId === uid;

                // Detail fields
                const od = (item as any).original_data || {};
                const rationale = od.rationale || item.rationale || od.assignment_reason || item.assignment_reason || '';
                const quote = od.original_quote || item.original_quote || '';
                const quoteTime = od.original_quote_timestamp || item.original_quote_timestamp || '';
                const assignees: string[] = od.assignees || item.assignees || [];
                const owner = od.owner || item.owner || '';
                const relatedLinks: string[] = od.related_links || item.related_links || [];

                return (
                  <div key={uid}>
                    <div
                      className={`v2-stream-item ${isCompleted ? 'completed' : ''} ${isDismissed ? 'dismissed' : ''} ${isExpanded ? 'expanded' : ''}`}
                      onClick={() => setExpandedItemId(isExpanded ? null : uid)}
                      style={{ cursor: 'pointer' }}
                    >
                      <span
                        className="v2-item-priority-dot"
                        style={{ backgroundColor: priorityDot(priority) }}
                        title={priority}
                      />

                      <div className="v2-item-body">
                        <span className={`v2-item-desc ${isCompleted || isDismissed ? 'strike' : ''}`}>
                          {description}
                        </span>
                        <div className="v2-item-meta">
                          {role && <span className={`v2-item-role role-${role}`}>{role}</span>}
                          {quoteTime && !/^(unknown|n\/?a|none|null)$/i.test(String(quoteTime).trim()) && (
                            <span className="v2-item-quote-time">{formatLocalTime(quoteTime)}</span>
                          )}
                          {deadline && !/^(unknown|n\/?a|none|null)$/i.test(String(deadline).trim()) && (
                            <span className="v2-item-deadline">Due: {deadline}</span>
                          )}
                          <span className="v2-item-expand-hint">{isExpanded ? '▾' : '▸'}</span>
                        </div>
                      </div>

                      <div className="v2-item-actions" onClick={(e) => e.stopPropagation()}>
                        <button
                          className={`v2-item-btn ${isCompleted ? 'undo' : 'complete'}`}
                          onClick={() => onComplete(uid)}
                          title={isCompleted ? 'Undo complete' : 'Mark complete'}
                        >
                          {isCompleted ? '↩' : '✓'}
                        </button>
                        <button
                          className={`v2-item-btn ${isDismissed ? 'undo' : 'dismiss'}`}
                          onClick={() => onDismiss(uid)}
                          title={isDismissed ? 'Undo dismiss' : 'Dismiss'}
                        >
                          {isDismissed ? '↩' : '×'}
                        </button>
                      </div>
                    </div>

                    {/* Expanded detail panel */}
                    {isExpanded && (
                      <div className="v2-item-detail">
                        {rationale && (
                          <div className="v2-detail-row">
                            <span className="v2-detail-label">Rationale</span>
                            <span className="v2-detail-value">{rationale}</span>
                          </div>
                        )}
                        {quote && (
                          <div className="v2-detail-row">
                            <span className="v2-detail-label">Original Quote</span>
                            <blockquote className="v2-detail-quote">
                              {quote}
                              {quoteTime && <cite className="v2-detail-quote-time">{formatLocalTime(quoteTime)}</cite>}
                            </blockquote>
                          </div>
                        )}
                        {(assignees.length > 0 || owner) && (
                          <div className="v2-detail-row">
                            <span className="v2-detail-label">{owner ? 'Owner' : 'Assignees'}</span>
                            <span className="v2-detail-value">{owner || assignees.join(', ')}</span>
                          </div>
                        )}
                        {relatedLinks.length > 0 && (
                          <div className="v2-detail-row">
                            <span className="v2-detail-label">Links</span>
                            <div className="v2-detail-links">
                              {relatedLinks.map((link, li) => (
                                <a key={li} href={link} target="_blank" rel="noopener noreferrer" className="v2-detail-link">{link}</a>
                              ))}
                            </div>
                          </div>
                        )}

                        {/* Open in Outlook / Teams button */}
                        <div className="v2-detail-actions">
                          {cardDeepLink && (
                            <a
                              href={cardDeepLink}
                              target="_blank"
                              rel="noopener noreferrer"
                              className="v2-detail-open-btn"
                            >
                              {isOutlook ? '📧 View in Outlook' : '💬 View in Teams'}
                            </a>
                          )}
                        </div>
                      </div>
                    )}
                  </div>
                );
              })}
            </div>
          </div>
        );
      })}
    </div>
  );
};
