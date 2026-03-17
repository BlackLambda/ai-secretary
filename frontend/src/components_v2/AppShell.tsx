/**
 * AppShell — New (v2) top-level wrapper.
 *
 * Differences from the legacy App layout:
 *   - No SideNav panel / resize handle
 *   - Compact header with overflow menu
 *   - Full-width card stream
 *   - Onboarding wizard for first-run
 */
import React, { useState, useEffect, useRef, useCallback, useMemo } from 'react';
import {
  BriefingData,
  PipelineStatus as PipelineStatusType,
  DashboardStats,
  Card,
  UserRole,
  DashboardFilter,
  DashboardStatus,
  PriorityLevelDefinition,
} from '../types';
import { api } from '../api';
import {
  extractPriorityLevelsFromScoringSystem,
  formatTimeChipText,
  generateUiId,
  getLatestCardTimestamp,
  sortCardsByPriority,
  sortCardsByDate,
  sortCardsByDeadlineUrgency,
  sortCardsByQuoteTimeLatest,
} from '../utils';
import { CompactHeader } from './CompactHeader';
import { ScheduleConfig } from './SchedulerPopover';
import { StatsBar } from './StatsBar';
import { CardStream } from './CardStream';
import { OnboardingWizard } from './OnboardingWizard';
import { SettingsDrawer } from './SettingsDrawer';
import './AppShell.css';

/* ------------------------------------------------------------------ */
/*  Helpers                                                            */
/* ------------------------------------------------------------------ */

/* ------------------------------------------------------------------ */
/*  Component                                                          */
/* ------------------------------------------------------------------ */

export const AppShell: React.FC = () => {
  // ---- Core data ----
  const [briefingData, setBriefingData] = useState<BriefingData | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [, setIsBriefingMissing] = useState(false);

  // ---- Pipeline ----
  const [pipelineStatus, setPipelineStatus] = useState<PipelineStatusType | null>(null);
  const [pipelineControlBusy, setPipelineControlBusy] = useState(false);

  // ---- User ops ----
  const [completedIds, setCompletedIds] = useState<Set<string>>(new Set());
  const [dismissedIds, setDismissedIds] = useState<Set<string>>(new Set());
  const [watchItems, setWatchItems] = useState<string[]>([]);
  const [, setPinnedCardIds] = useState<string[]>([]);

  // ---- Display ----
  const [activeDatasetPath, setActiveDatasetPath] = useState('');
  const [activeUserDisplayName, setActiveUserDisplayName] = useState('');
  const [sortedCards, setSortedCards] = useState<Card[] | null>(null);
  const [activeSort, setActiveSort] = useState<'priority' | 'updated' | 'deadline' | 'received'>('priority');
  const [priorityLevels, setPriorityLevels] = useState<PriorityLevelDefinition[] | null>(null);

  // ---- AI provider/model (shown in header) ----
  const [aiProvider, setAiProvider] = useState<string>('azure');
  const [aiModel, setAiModel] = useState<string>('');

  // ---- Banned event types (persisted to localStorage) ----
  const BANNED_TYPES_KEY = 'ai_secretary.banned_event_types';
  const [bannedTypes, setBannedTypes] = useState<Set<string>>(() => {
    try {
      const raw = localStorage.getItem(BANNED_TYPES_KEY);
      if (raw) return new Set(JSON.parse(raw) as string[]);
    } catch { /* ignore */ }
    return new Set();
  });
  const handleBannedTypesChange = useCallback((next: Set<string>) => {
    setBannedTypes(next);
    try { localStorage.setItem(BANNED_TYPES_KEY, JSON.stringify([...next])); } catch { /* ignore */ }
  }, []);

  // Collect all unique event types across cards
  const allEventTypes = useMemo(() => {
    const types = new Set<string>();
    for (const card of briefingData?.cards || []) {
      if (card.type === 'Outlook') {
        const et = (card.data as any)?.event_type;
        if (et && typeof et === 'string') types.add(et);
      }
    }
    return [...types].sort();
  }, [briefingData?.cards]);

  // ---- Dashboard filter (simplified — no SideNav tag filters) ----
  const [dashboardSelection, setDashboardSelection] = useState<{ status: DashboardStatus; role: UserRole | null }>({
    status: 'active',
    role: 'assignee',
  });

  const dashboardFilter = useMemo<DashboardFilter>(() => {
    const role = dashboardSelection.status === 'active' ? dashboardSelection.role : null;
    return {
      status: dashboardSelection.status,
      role,
      sources: { outlook: true, teams: true },
      priorities: { High: true, Medium: true, Low: true },
      eventTypes: {},
    };
  }, [dashboardSelection]);

  // ---- Onboarding ----
  const [showOnboarding, setShowOnboarding] = useState(false);

  // ---- Update banner ----
  const [updateInfo, setUpdateInfo] = useState<{ has_update: boolean; server_stale?: boolean; current?: string; latest?: string; server_commit?: string; message?: string } | null>(null);
  const [updateDismissed, setUpdateDismissed] = useState(false);

  useEffect(() => {
    let cancelled = false;
    const check = async () => {
      try {
        const info = await api.getAppUpdateStatus();
        if (!cancelled) setUpdateInfo(info);
      } catch { /* ignore */ }
    };
    check();
    // Re-check every minute so remote pushes surface promptly.
    const id = window.setInterval(check, 60_000);
    return () => { cancelled = true; window.clearInterval(id); };
  }, []);

  useEffect(() => {
    if (updateInfo?.has_update || updateInfo?.server_stale) {
      setUpdateDismissed(false);
    }
  }, [updateInfo?.current, updateInfo?.latest, updateInfo?.server_commit, updateInfo?.has_update, updateInfo?.server_stale]);

  // ---- AI error banner ----
  const [aiErrors, setAiErrors] = useState<Array<{ timestamp: string; message: string }>>([]);
  const [aiErrorsDismissed, setAiErrorsDismissed] = useState(false);
  // Initialise to "now" so stale errors from previous sessions are never shown.
  const lastAiErrorTimestampRef = useRef<string>(new Date().toISOString());

  useEffect(() => {
    let cancelled = false;
    const poll = async () => {
      try {
        const errors = await api.getAiErrors(lastAiErrorTimestampRef.current || undefined);
        if (!cancelled && errors.length > 0) {
          setAiErrors((prev) => [...prev, ...errors]);
          setAiErrorsDismissed(false);
          lastAiErrorTimestampRef.current = errors[errors.length - 1].timestamp;
        }
      } catch { /* ignore */ }
    };
    poll();
    const id = window.setInterval(poll, 30_000);
    return () => { cancelled = true; window.clearInterval(id); };
  }, []);

  // ---- Browser extension detection ----
  const [extensionInstalled, setExtensionInstalled] = useState(false);
  useEffect(() => {
    const check = () => document.documentElement.getAttribute('data-ai-secretary-ext') === '1';
    if (check()) { setExtensionInstalled(true); return; }
    const handler = (e: MessageEvent) => {
      if (e.data?.type === '__AI_SECRETARY_EXT_INSTALLED') setExtensionInstalled(true);
    };
    window.addEventListener('message', handler);
    const timer = setTimeout(() => { if (check()) setExtensionInstalled(true); }, 1000);
    return () => { window.removeEventListener('message', handler); clearTimeout(timer); };
  }, []);

  const onboardingDoneRef = useRef(false);

  // ---- Settings drawer ----
  const [showSettings, setShowSettings] = useState(false);
  const handleOpenSettings = useCallback(() => setShowSettings(true), []);
  const handleCloseSettings = useCallback(() => setShowSettings(false), []);

  /* ---------------------------------------------------------------- */
  /*  Schedule config                                                  */
  /* ---------------------------------------------------------------- */

  const DEFAULT_SCHEDULE: ScheduleConfig = {
    schedule_enabled: false,
    schedule_days: [1, 2, 3, 4, 5],
    schedule_start_hour: 8,
    schedule_start_minute: 0,
    schedule_end_hour: 17,
    schedule_end_minute: 0,
    fetch_interval_minutes: 60,
  };

  const [scheduleConfig, setScheduleConfig] = useState<ScheduleConfig>(DEFAULT_SCHEDULE);

  // Load schedule config on mount
  useEffect(() => {
    (async () => {
      try {
        const { config } = await api.getPipelineConfig();
        if (config) {
          setScheduleConfig({
            schedule_enabled: !!config.schedule_enabled,
            schedule_days: Array.isArray(config.schedule_days) ? config.schedule_days : [1, 2, 3, 4, 5],
            schedule_start_hour: typeof config.schedule_start_hour === 'number' ? config.schedule_start_hour : 8,
            schedule_start_minute: typeof config.schedule_start_minute === 'number' ? config.schedule_start_minute : 0,
            schedule_end_hour: typeof config.schedule_end_hour === 'number' ? config.schedule_end_hour : 17,
            schedule_end_minute: typeof config.schedule_end_minute === 'number' ? config.schedule_end_minute : 0,
            fetch_interval_minutes: typeof config.fetch_interval_minutes === 'number' ? config.fetch_interval_minutes : 60,
          });
        }
      } catch { /* ignore */ }
    })();
  }, []);

  const handleSaveSchedule = useCallback(async (updates: Partial<ScheduleConfig>) => {
    await api.savePipelineConfig(updates);
    setScheduleConfig((prev) => ({ ...prev, ...updates }));
  }, []);

  /* ---------------------------------------------------------------- */
  /*  Pipeline status polling                                          */
  /* ---------------------------------------------------------------- */

  const pipelineCanStart = !pipelineStatus || pipelineStatus.state === 'offline';
  const pipelineIsSleeping = pipelineStatus?.state === 'sleeping';

  const refreshPipelineStatus = useCallback(async () => {
    try {
      const status = await api.getPipelineStatus();
      setPipelineStatus(status);
    } catch {
      // ignore
    }
  }, []);

  // Poll pipeline status — faster (3s) while working, slower (10s) otherwise
  const pipelinePollingRef = useRef<number | null>(null);
  useEffect(() => {
    let cancelled = false;
    const pollInterval = (pipelineStatus?.state === 'working') ? 3_000 : 10_000;
    const poll = async () => {
      try {
        const status = await api.getPipelineStatus();
        if (!cancelled) setPipelineStatus(status);
      } catch {
        // ignore
      }
    };
    poll();
    pipelinePollingRef.current = window.setInterval(poll, pollInterval);
    return () => { cancelled = true; window.clearInterval(pipelinePollingRef.current!); };
  }, [pipelineStatus?.state]);

  /* ---------------------------------------------------------------- */
  /*  Load briefing data                                               */
  /* ---------------------------------------------------------------- */

  const processDataWithUiIds = (data: BriefingData) => {
    if (!data?.cards) return;
    let idx = 0;
    for (const card of data.cards) {
      if (card.type === 'Outlook') {
        const event: any = card.data as any;
        for (const list of [event?.todos, event?.recommendations]) {
          if (!Array.isArray(list)) continue;
          for (const item of list) {
            if (!item._ui_id) item._ui_id = generateUiId('item', idx++);
          }
        }
      } else {
        const teams: any = card.data as any;
        for (const list of [teams?.linked_items, teams?.unlinked_items, teams?.conversation?.tasks]) {
          if (!Array.isArray(list)) continue;
          for (const item of list) {
            if (!item._ui_id) item._ui_id = generateUiId('item', idx++);
          }
        }
      }
    }
  };

  const loadBriefingData = useCallback(async () => {
    try {
      setLoading(true);
      setError(null);
      const response = await api.getBriefingData();
      setIsBriefingMissing(false);
      setShowOnboarding(false);

      if (response.user_ops) {
        setCompletedIds(new Set(response.user_ops.completed || []));
        setDismissedIds(new Set(response.user_ops.dismissed || []));
        const pinned = (response.user_ops as any)?.pinned_cards;
        setPinnedCardIds(Array.isArray(pinned) ? pinned.map((x: any) => String(x || '').trim()).filter(Boolean) : []);
      }

      if (Array.isArray((response.user_profile as any)?.following)) {
        setWatchItems((response.user_profile as any).following);
      }

      processDataWithUiIds(response);
      setBriefingData(response);
    } catch (err: any) {
      if (err?.status === 404) {
        setIsBriefingMissing(true);
        setError(null);
        setBriefingData({ cards: [], history_map: {}, teams_history_map: {}, chat_lookup: {}, outlook_threads_lookup: {}, user_ops: { completed: [], dismissed: [] }, user_profile: { following: [] }, card_feedback: {}, focus_scores: {} });
        setSortedCards([]);

        const state = pipelineStatus?.state ?? 'offline';
        if (state === 'offline' && !pipelineControlBusy && !onboardingDoneRef.current) {
          setShowOnboarding(true);
        }
      } else {
        setError(err?.message || 'Failed to load briefing data');
      }
    } finally {
      setLoading(false);
    }
  }, [pipelineStatus?.state, pipelineControlBusy]);

  useEffect(() => { loadBriefingData(); }, []);

  // Re-load when pipeline transitions from working → sleeping
  const lastPipelineStateRef = useRef<string | null>(null);
  useEffect(() => {
    const curr = pipelineStatus?.state ?? null;
    const prev = lastPipelineStateRef.current;
    lastPipelineStateRef.current = curr;

    if (prev === 'working' && curr === 'sleeping') {
      loadBriefingData();
    }
  }, [pipelineStatus?.state]);

  /* ---------------------------------------------------------------- */
  /*  App config                                                       */
  /* ---------------------------------------------------------------- */

  useEffect(() => {
    (async () => {
      try {
        const appCfg: any = await api.getAppConfig();
        setActiveDatasetPath(String(appCfg?.active_data_folder || '').trim());
        setActiveUserDisplayName(String(appCfg?.user_display_name || '').trim());
      } catch { /* ignore */ }

      try {
        const cfg: any = await api.getPipelineConfig();
        const levels = extractPriorityLevelsFromScoringSystem(cfg?.scoring_system);
        if (levels) setPriorityLevels(levels);
        // Sync AI provider/model into header
        const c = cfg?.config || cfg;
        const backend = (c?.ai_backend || '').toString().toLowerCase();
        if (backend === 'azure' || backend === 'copilot') setAiProvider(backend);
        const activeModel = backend === 'copilot'
          ? (c?.copilot_model || '').toString().trim()
          : (c?.azure_model || '').toString().trim();
        if (activeModel) setAiModel(activeModel);
      } catch { /* ignore */ }
    })();
  }, []);

  /* ---------------------------------------------------------------- */
  /*  Sorting                                                          */
  /* ---------------------------------------------------------------- */

  const watchItemsLower = useMemo(() =>
    new Set((watchItems || []).map((w) => String(w).toLowerCase())),
    [watchItems],
  );

  useEffect(() => {
    if (!briefingData?.cards) return;
    const opts = { watchItemsLower, taskTodoScoreCutoff: 0, priorityLevels };
    let sorted: Card[];
    switch (activeSort) {
      case 'updated':
        sorted = sortCardsByDate(briefingData.cards, completedIds, dismissedIds, opts);
        break;
      case 'deadline':
        sorted = sortCardsByDeadlineUrgency(briefingData.cards, completedIds, dismissedIds, opts);
        break;
      case 'received':
        sorted = sortCardsByQuoteTimeLatest(briefingData.cards);
        break;
      default:
        sorted = sortCardsByPriority(briefingData.cards, completedIds, dismissedIds, opts);
    }
    setSortedCards(sorted);
  }, [briefingData, activeSort, completedIds, dismissedIds, watchItems, priorityLevels]);

  /* ---------------------------------------------------------------- */
  /*  Stats                                                            */
  /* ---------------------------------------------------------------- */

  const normalizeUserRole = (raw: unknown): UserRole | undefined => {
    const norm = new Set(
      String(raw ?? '').split(/[^a-zA-Z]+/).map((s) => s.trim().toLowerCase()).filter(Boolean),
    );
    if (norm.has('assignee')) return 'assignee';
    if (norm.has('collaborator')) return 'collaborator';
    if (norm.has('observer')) return 'observer';
    return undefined;
  };

  const getAllItems = useCallback((card: Card) => {
    if (card.type === 'Outlook') {
      const ev: any = card.data;
      return [...(ev?.todos || []), ...(ev?.recommendations || [])];
    }
    const t: any = card.data;
    return [...(t?.linked_items || []), ...(t?.unlinked_items || []), ...(t?.conversation?.tasks || [])];
  }, []);

  const dashboardStats = useMemo<DashboardStats>(() => {
    const allCards = briefingData?.cards || [];

    // Filter out cards whose event_type is banned (same logic as CardStream)
    const cards = allCards.filter((card) => {
      if (bannedTypes && bannedTypes.size > 0 && card.type === 'Outlook') {
        const et = (card.data as any)?.event_type;
        if (et && typeof et === 'string' && bannedTypes.has(et)) return false;
      }
      return true;
    });

    let assignee = 0, collaborator = 0, observer = 0, watching = 0;
    let totalActive = 0, totalCompleted = 0, totalDismissed = 0;

    for (const card of cards) {
      const items = getAllItems(card);
      const cardRoles = new Set<string>();
      let cardHasActive = false;

      for (const it of items) {
        const uid = it._ui_id || '';
        if (completedIds.has(uid)) { totalCompleted++; continue; }
        if (dismissedIds.has(uid)) { totalDismissed++; continue; }
        totalActive++;
        cardHasActive = true;
        const role = normalizeUserRole(it.original_data?.user_role ?? it.user_role);
        if (role) cardRoles.add(role);
      }

      // Count cards (tasks), not individual items
      if (cardHasActive) {
        if (cardRoles.has('assignee')) assignee++;
        if (cardRoles.has('collaborator')) collaborator++;
        if (cardRoles.has('observer')) observer++;
        if (!cardRoles.has('assignee') && !cardRoles.has('collaborator') && !cardRoles.has('observer')) watching++;
      }
    }

    return {
      outlookCount: cards.filter((c) => c.type === 'Outlook').length,
      teamsCount: cards.filter((c) => c.type === 'Teams').length,
      totalActiveCount: totalActive,
      activeAssigneeCount: assignee,
      activeCollaboratorCount: collaborator,
      activeObserverCount: observer,
      activeWatchingCount: watching,
      totalCompletedCount: totalCompleted,
      totalDismissedCount: totalDismissed,
      doneTabCompletedCount: totalCompleted,
      doneTabDismissedCount: totalDismissed,
      totalRecCount: 0,
      noActionCount: 0,
    };
  }, [briefingData, completedIds, dismissedIds, getAllItems, bannedTypes]);

  /* ---------------------------------------------------------------- */
  /*  Pipeline control                                                 */
  /* ---------------------------------------------------------------- */

  const handleTogglePipeline = async () => {
    if (pipelineControlBusy) return;
    try {
      setPipelineControlBusy(true);
      if (pipelineCanStart || pipelineIsSleeping) {
        // For sleeping: stop then restart to force immediate re-run
        if (pipelineIsSleeping) {
          try { await api.stopPipeline(); } catch { /* ignore */ }
          // Brief wait for process to terminate
          await new Promise((r) => setTimeout(r, 1500));
        }
        await api.startPipeline();
      } else {
        await api.stopPipeline();
      }
      window.setTimeout(async () => {
        try { setPipelineStatus(await api.getPipelineStatus()); } catch { /* */ }
      }, 800);
    } catch (e: any) {
      window.alert(e?.message || 'Pipeline action failed');
    } finally {
      setPipelineControlBusy(false);
    }
  };

  /* ---------------------------------------------------------------- */
  /*  User operations                                                  */
  /* ---------------------------------------------------------------- */

  const handleComplete = useCallback(async (itemId: string) => {
    const isCompleted = completedIds.has(itemId);
    try {
      await api.saveOperation('complete', itemId, !isCompleted);
      setCompletedIds((prev) => {
        const next = new Set(prev);
        isCompleted ? next.delete(itemId) : next.add(itemId);
        return next;
      });
    } catch { /* ignore */ }
  }, [completedIds]);

  const handleDismiss = useCallback(async (itemId: string) => {
    const isDismissed = dismissedIds.has(itemId);
    try {
      await api.saveOperation('dismiss', itemId, !isDismissed);
      setDismissedIds((prev) => {
        const next = new Set(prev);
        isDismissed ? next.delete(itemId) : next.add(itemId);
        return next;
      });
    } catch { /* ignore */ }
  }, [dismissedIds]);

  /* ---------------------------------------------------------------- */
  /*  Last-updated label                                               */
  /* ---------------------------------------------------------------- */

  const briefingLastUpdatedLabel = useMemo(() => {
    const pipelineTs = pipelineStatus?.last_updated;
    const pipelineLabel = formatTimeChipText('update', pipelineTs);
    if (pipelineLabel) return pipelineLabel;

    const cards = briefingData?.cards || [];
    let maxMs = 0;
    for (const card of cards) {
      const ms = getLatestCardTimestamp(card);
      if (ms > maxMs) maxMs = ms;
    }
    return formatTimeChipText('update', maxMs > 0 ? new Date(maxMs).toISOString() : null) || 'Updated: Unknown';
  }, [briefingData?.cards, pipelineStatus?.last_updated]);

  /* ---------------------------------------------------------------- */
  /*  Pipeline status view helpers                                     */
  /* ---------------------------------------------------------------- */

  // Live countdown for sleeping state
  const [countdownLabel, setCountdownLabel] = useState('');

  useEffect(() => {
    const state = pipelineStatus?.state;
    const nextRun = pipelineStatus?.next_run;
    if (state !== 'sleeping' || !nextRun) {
      setCountdownLabel('');
      return;
    }

    const tick = () => {
      const target = new Date(nextRun).getTime();
      const now = Date.now();
      const diff = Math.max(0, Math.round((target - now) / 1000));
      if (diff <= 0) {
        setCountdownLabel('Starting soon…');
        return;
      }
      const m = Math.floor(diff / 60);
      const s = diff % 60;
      setCountdownLabel(`Next run in ${m}:${String(s).padStart(2, '0')}`);
    };

    tick();
    const id = window.setInterval(tick, 1000);
    return () => window.clearInterval(id);
  }, [pipelineStatus?.state, pipelineStatus?.next_run]);

  const pipelineStatusView = useMemo(() => {
    const state = pipelineStatus?.state ?? 'offline';
    const steps = pipelineStatus?.steps ?? [];
    const currentStepId = pipelineStatus?.current_step_id ?? '';

    let stageLabel = '';
    let progress = 0;

    if (state === 'working' && steps.length > 0) {
      const total = steps.length;
      const doneCount = steps.filter((s) => s.status === 'ok' || s.status === 'error').length;
      const running = steps.find((s) => s.status === 'running') || (currentStepId ? steps.find((s) => s.id === currentStepId) : null);
      progress = total > 0 ? Math.round((doneCount / total) * 100) : 0;
      // If something is running, count it as partial
      if (running && progress < 100) {
        progress = Math.round(((doneCount + 0.5) / total) * 100);
      }
      stageLabel = running?.name || (doneCount === total ? 'Finishing…' : '');
    }

    if (state === 'working') return { color: '#27ae60', label: 'Running', pulse: true, stageLabel, progress };
    if (state === 'sleeping') return { color: '#f39c12', label: countdownLabel || 'Sleeping', pulse: false, stageLabel: '', progress: 100 };
    return { color: '#95a5a6', label: 'Offline', pulse: false, stageLabel: '', progress: 0 };
  }, [pipelineStatus?.state, pipelineStatus?.steps, pipelineStatus?.current_step_id, countdownLabel]);

  const handleDataReset = useCallback(() => {
    // Clear onboarding localStorage so wizard starts fresh
    try { localStorage.removeItem('ai_secretary.onboarding'); } catch { /* ignore */ }
    onboardingDoneRef.current = false;
    setShowSettings(false);
    setShowOnboarding(true);
  }, []);

  /* ---------------------------------------------------------------- */
  /*  Onboarding complete handler                                      */
  /* ---------------------------------------------------------------- */

  const handleOnboardingComplete = useCallback(() => {
    onboardingDoneRef.current = true;
    setShowOnboarding(false);
    // Poll for briefing data after pipeline starts (retry until available)
    const poll = () => {
      loadBriefingData();
    };
    const id = window.setInterval(poll, 8000);
    window.setTimeout(poll, 3000);
    // Stop polling after 10 minutes
    window.setTimeout(() => window.clearInterval(id), 600_000);
  }, [loadBriefingData]);

  /* ---------------------------------------------------------------- */
  /*  Render                                                           */
  /* ---------------------------------------------------------------- */

  // Loading
  if (loading && !briefingData) {
    return (
      <div className="v2-loading">
        <div className="v2-loading-spinner" />
        <p>Loading briefing…</p>
      </div>
    );
  }

  // Error
  if (error && !briefingData) {
    return (
      <div className="v2-loading">
        <div className="v2-error">
          <h2>Something went wrong</h2>
          <p>{error}</p>
          <button className="v2-btn v2-btn-primary" onClick={loadBriefingData}>Retry</button>
        </div>
      </div>
    );
  }

  // Onboarding wizard
  if (showOnboarding) {
    return <OnboardingWizard onComplete={handleOnboardingComplete} />;
  }

  // Main dashboard (no SideNav)
  const cards = sortedCards || briefingData?.cards || [];

  return (
    <div className="v2-app">
      <CompactHeader
        title="AI Secretary"
        subtitle={briefingLastUpdatedLabel}
        datasetPath={activeDatasetPath}
        userName={activeUserDisplayName}
        aiProvider={aiProvider}
        aiModel={aiModel}
        pipelineStatus={pipelineStatusView}
        pipelineCanStart={pipelineCanStart}
        pipelineIsSleeping={pipelineIsSleeping}
        pipelineControlBusy={pipelineControlBusy}
        onTogglePipeline={handleTogglePipeline}
        onRefresh={loadBriefingData}
        onSettings={handleOpenSettings}
        loading={loading}
        extensionInstalled={extensionInstalled}
        scheduleConfig={scheduleConfig}
        onSaveSchedule={handleSaveSchedule}
      />

      {/* Update banner */}
      {(updateInfo?.has_update || updateInfo?.server_stale) && !updateDismissed && (
        <div className="v2-update-banner">
          <div className="v2-update-banner-content">
            <span className="v2-update-icon">🔄</span>
            <div className="v2-update-text">
              <span className="v2-update-title">
                {updateInfo.has_update ? 'Update available' : 'Server restart needed'}
              </span>
              <span className="v2-update-detail">
                {updateInfo.has_update
                  ? (updateInfo.message
                      ? `${updateInfo.current} → ${updateInfo.latest}: ${updateInfo.message}`
                      : `${updateInfo.current} → ${updateInfo.latest}`)
                  : `Running ${updateInfo.server_commit || '?'}, current code is ${updateInfo.current || '?'}`}
              </span>
            </div>
          </div>
          <div className="v2-update-actions">
            <button
              className="v2-update-restart-btn"
              onClick={() => {
                try {
                  const frame = document.createElement('iframe');
                  frame.style.display = 'none';
                  frame.src = 'ai-secretary-update://restart';
                  document.body.appendChild(frame);
                  setTimeout(() => frame.remove(), 3000);
                } catch { window.open('ai-secretary-update://restart'); }
              }}
            >
              Update &amp; Restart
            </button>
            <button className="v2-update-dismiss-btn" onClick={() => setUpdateDismissed(true)} title="Dismiss">✕</button>
          </div>
        </div>
      )}

      {/* AI errors banner */}
      {aiErrors.length > 0 && !aiErrorsDismissed && (
        <div className="v2-ai-error-banner">
          <span className="v2-ai-error-icon">⚠️</span>
          <div className="v2-ai-error-text">
            <strong>AI call error</strong>
            <span className="v2-ai-error-detail">{aiErrors[aiErrors.length - 1].message}</span>
            {aiErrors.length > 1 && (
              <span className="v2-ai-error-count">+{aiErrors.length - 1} more</span>
            )}
          </div>
          <button
            className="v2-ai-error-dismiss"
            onClick={() => setAiErrorsDismissed(true)}
            title="Dismiss"
          >✕</button>
        </div>
      )}

      <StatsBar
        stats={dashboardStats}
        activeStatus={dashboardSelection.status}
        activeRole={dashboardSelection.role}
        onStatusChange={(status) => setDashboardSelection((p) => ({ ...p, status, role: status === 'active' ? p.role : null }))}
        onRoleChange={(role) => setDashboardSelection({ status: 'active', role })}
      />

      <div className="v2-sort-bar">
        {(['priority', 'updated', 'deadline', 'received'] as const).map((s) => {
          const label: Record<string, string> = {
            priority: 'Priority',
            updated: 'Detected Time',
            deadline: 'Deadline',
            received: 'Received Time',
          };
          return (
            <button
              key={s}
              className={`v2-sort-chip ${activeSort === s ? 'active' : ''}`}
              onClick={() => setActiveSort(s)}
            >
              {label[s]}
            </button>
          );
        })}
      </div>

      <CardStream
        cards={cards}
        completedIds={completedIds}
        dismissedIds={dismissedIds}
        dashboardFilter={dashboardFilter}
        priorityLevels={priorityLevels}
        briefingData={briefingData}
        bannedTypes={bannedTypes}
        onComplete={handleComplete}
        onDismiss={handleDismiss}
      />

      <SettingsDrawer
        open={showSettings}
        onClose={handleCloseSettings}
        onPipelineAction={refreshPipelineStatus}
        onDataReset={handleDataReset}
        bannedTypes={bannedTypes}
        allEventTypes={allEventTypes}
        onBannedTypesChange={handleBannedTypesChange}
        onAiConfigChange={(provider, model) => { setAiProvider(provider); setAiModel(model); }}
      />
    </div>
  );
};
