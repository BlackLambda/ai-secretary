// Core Types
export type Priority = 'High' | 'Medium' | 'Low';

export type PriorityLevelDefinition = {
  label: Priority;
  min_score: number;
};
export type UserRole = 'assignee' | 'collaborator' | 'observer';
export type ItemStatus = 'active' | 'completed' | 'dismissed';
export type DashboardStatus = 'active' | 'done';
export type FilterType = 'all' | 'done' | 'priority' | 'source';
export type CardType = 'Outlook' | 'Teams';

export type DashboardFilter = {
  status: DashboardStatus;
  sources: { outlook: boolean; teams: boolean };
  priorities: Record<Priority, boolean>;
  // Outlook-only filter. Keys are normalized (lowercased, trimmed) event types.
  // Missing key implies selected (included).
  eventTypes: Record<string, boolean>;
  role: UserRole | null;
};

// Action Item
export interface ActionItem {
  description?: string;
  task?: string;  // Alternative field name for description
  priority?: Priority;
  priority_score?: number;
  priority_score_max?: number;
  scoring_breakdown?: Record<string, number>;
  scoring_evidence?: Record<string, string | string[]>;
  assignees?: string[];
  owner?: string;
  original_quote?: string;
  original_quote_timestamp?: string;
  rationale?: string;
  assignment_reason?: string;
  user_role?: UserRole;
  deadline?: string | null;
  related_links?: string[];
  last_updated?: string;
  link?: string;
  chat_name?: string;
  chat_id?: string;
  origin?: string;
  type?: 'Task' | 'Action';
  _ui_id?: string;
  original_data?: {
    task?: string;
    description?: string;
    priority?: Priority;
    priority_score?: number;
    priority_score_max?: number;
    scoring_breakdown?: Record<string, number>;
    scoring_evidence?: Record<string, string | string[]>;
    assignees?: string[];
    owner?: string;
    original_quote?: string;
    original_quote_timestamp?: string;
    rationale?: string;
    assignment_reason?: string;
    user_role?: UserRole;
    deadline?: string | null;
    related_links?: string[];
    last_updated?: string;
  };
}

// Outlook Event
export interface OutlookEvent {
  event_name: string;
  event_id: string;
  event_type?: string;
  action_summary?: string;
  summary?: string;
  priority_level?: Priority;
  labels?: string[];
  dedup_merge_info?: Array<{
    event_id: string;
    event_name?: string;
    reason?: string;
  }>;
  last_updated?: string;
  attendees?: string[];
  key_participants?: string[];
  participants?: string[];
  key_outcomes?: string[];
  timeline?: Array<string | { date: string; description: string }>;
  executive_summary?: string;
  description?: string;
  web_link?: string;
  weblink?: string;
  todos?: ActionItem[];
  recommendations?: ActionItem[];
  threads?: any[];
  related_threads?: Array<{
    messages?: Array<{
      WebLink?: string;
    }>;
  }>;
}

// Teams Item
export interface TeamsItem {
  conversation: {
    chat_name: string;
    chat_id: string;
    conversation_id?: string;
    last_updated?: string;
    // Some pipelines emit extracted tasks here (instead of linked_items/unlinked_items).
    tasks?: ActionItem[];
    summary?: {
      topic?: string;
      action_summary?: string;
      key_points?: string[];
      decisions_made?: string[];
    };
  };
  last_updated?: string;
  linked_items?: ActionItem[];
  unlinked_items?: ActionItem[];
}

// Card
export interface Card {
  type: CardType;
  data: OutlookEvent | TeamsItem;
  priority?: string;
}

// Briefing Data
export interface BriefingData {
  cards: Card[];
  history_map?: Record<string, any>;
  teams_history_map?: Record<string, any>;
  chat_lookup?: Record<string, any>;
  outlook_threads_lookup?: Record<string, any>;
  user_ops?: UserOperations;
  // Deprecated legacy fields from the removed per-card like/dislike flow.
  // Keys are stable card ids: "outlook|<stableKey>" or "teams|<stableKey>".
  /** @deprecated Legacy per-card like/dislike state. */
  card_feedback?: Record<string, 'like' | 'dislike'>;
  /** @deprecated Legacy feedback-derived focus scores. */
  focus_scores?: Record<string, number>;
  user_profile?: {
    following?: string[];
  };
}

// User Operations
export interface UserOperations {
  completed: string[];
  dismissed: string[];
  // Persisted UI preference: per-list excluded Outlook event_type keys.
  // listKey format: "active|assignee", "active|observer", "done|".
  disabled_event_types_by_list?: Record<string, string[]>;
  // Persisted UI preference: pinned cards (stable card ids: "outlook|<key>" / "teams|<key>")
  pinned_cards?: string[];
}

// Pipeline Status
export type PipelineState = 'working' | 'sleeping' | 'offline';

export type PipelineStepStatus = 'queued' | 'running' | 'ok' | 'error';

export interface PipelineStep {
  id: string;
  index?: number;
  name?: string;
  status?: PipelineStepStatus;
  started_at?: string;
  ended_at?: string;
  command?: string;
  // Optional: per-step IO artifacts for UI inspection.
  // Paths are typically repo-relative or absolute (best-effort).
  input_files?: string[];
  output_files?: string[];
  // Optional: mapping of output path -> pre-step snapshot path (best-effort).
  // Used to show deltas when a step modifies a file in-place.
  before_files?: Record<string, string>;
  exit_code?: number;
  error?: string;
}

export interface PipelineStatus {
  state: PipelineState;
  message?: string;
  next_run?: string;

  // Extended fields (best-effort) used by the Observation page.
  last_updated?: string;
  run_id?: string;
  current_step_id?: string;
  steps?: PipelineStep[];
}

// Stats
export interface DashboardStats {
  outlookCount: number;
  teamsCount: number;
  totalActiveCount: number;
  activeAssigneeCount: number;
  activeCollaboratorCount: number;
  activeObserverCount: number;
  activeWatchingCount: number;
  totalCompletedCount: number;
  totalDismissedCount: number;
  // Done tab display counts (scoped to the Done list's tag filters).
  doneTabCompletedCount: number;
  doneTabDismissedCount: number;
  totalRecCount: number;
  noActionCount: number;
}

// Navigation
export interface NavigationFile {
  path: string;
  label: string;
  timestamp?: string;
  is_snapshot?: boolean;
}

// Nav Item
export interface NavItem {
  ui_id: string;
  desc: string;
  priority: Priority;
  user_role?: UserRole;
  source: 'Outlook' | 'Teams';
  type: 'task' | 'rec';
  status: ItemStatus;
  timestamp?: string;
}

// Card Item for rendering
export interface CardItem {
  item: ActionItem;
  cardType: CardType;
  cardTitle: string;
}

// Bug Report Types
export type ActionBugType = 'not_assigned_to_me' | 'incorrect_priority' | 'wrong_description' | 'other';
export type CardBugType = 'duplicated_actions' | 'unrelated_source_combined' | 'missing_info' | 'other';
export type CardSection = 'actions' | 'details' | 'participants';

export interface BugReport {
  bug_type: ActionBugType | CardBugType;
  item_id?: string;
  card_type?: CardType;
  card_index?: number;
  timestamp: string;
  raw_data: any;
  sections_with_bugs?: CardSection[];
  selected_action_ids?: string[];
}
