/**
 * SettingsDrawer — Slide-over settings panel for the v2 UI.
 *
 * Covers the most common settings:
 *  • User Profile (name, alias, email)
 *  • Followed Topics
 *  • Pipeline Config (key fields)
 *
 * Uses the same API endpoints as the v1 settings modal.
 */
import React, { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { api } from '../api';
import './SettingsDrawer.css';

/* ------------------------------------------------------------------ */
/*  Types                                                              */
/* ------------------------------------------------------------------ */
interface Props {
  open: boolean;
  onClose: () => void;
  /** Called after a data operation that restarts the pipeline (reset / rerun). */
  onPipelineAction?: () => void;
  /** Called after a full data reset — triggers the onboarding wizard. */
  onDataReset?: () => void;
  /** Set of event types currently banned (hidden from card stream). */
  bannedTypes?: Set<string>;
  /** Called when AI provider/model config changes (after save). */
  onAiConfigChange?: (provider: string, model: string) => void;
  /** All known event types from the current briefing data. */
  allEventTypes?: string[];
  /** Called when user toggles a banned type. */
  onBannedTypesChange?: (next: Set<string>) => void;
}

type Tab = 'profile' | 'topics' | 'ai' | 'config' | 'data';

const DEFAULT_COPILOT_MODEL = 'gemini-3-flash-preview';

const prioritizeDefaultModel = (models: string[], defaultModel: string): string[] => {
  const unique = Array.from(new Set((models || []).filter(Boolean)));
  if (unique.includes(defaultModel)) {
    return [defaultModel, ...unique.filter((m) => m !== defaultModel)];
  }
  return [defaultModel, ...unique];
};

const formatModelOptionLabel = (model: string, provider: 'azure' | 'copilot'): string => {
  if (provider === 'copilot' && model === DEFAULT_COPILOT_MODEL) {
    return `★ ${model} (default)`;
  }
  return model;
};

/* ------------------------------------------------------------------ */
/*  Component                                                          */
/* ------------------------------------------------------------------ */
export const SettingsDrawer: React.FC<Props> = ({ open, onClose, onPipelineAction, onDataReset, onAiConfigChange, bannedTypes, allEventTypes, onBannedTypesChange }) => {
  const [tab, setTab] = useState<Tab>('profile');

  /* ---- Profile ---- */
  const [profileLoading, setProfileLoading] = useState(false);
  const [profileSaving, setProfileSaving] = useState(false);
  const [profileError, setProfileError] = useState<string | null>(null);
  const [profileForm, setProfileForm] = useState<Record<string, string>>({});
  const [profileLoaded, setProfileLoaded] = useState(false);

  const loadProfile = useCallback(async () => {
    if (profileLoaded) return;
    setProfileLoading(true);
    setProfileError(null);
    try {
      const { profile } = await api.getUserProfile();
      const p = profile || {};
      // Backend stores arrays; extract first element as display string.
      const first = (v: unknown) => (Array.isArray(v) ? v[0] ?? '' : v ?? '');
      setProfileForm({
        USER_NAME: String(first(p.USER_NAME)),
        USER_ALIAS: String(first(p.USER_ALIAS)),
        USER_EMAIL: String(first(p.USER_EMAIL)),
        TEAM_NAME: String(first(p.USER_TEAM)),
        MANAGER_NAME: String(first(p.MANAGER_INFO)),
        MANAGER_ALIAS: '',
        ORG_NAME: String(first(p.USER_COMPANY)),
      });
      setProfileLoaded(true);
    } catch (err: any) {
      setProfileError(err.message || 'Failed to load profile');
    } finally {
      setProfileLoading(false);
    }
  }, [profileLoaded]);

  const saveProfile = useCallback(async () => {
    setProfileSaving(true);
    setProfileError(null);
    try {
      // Map display field names back to backend array fields
      const payload: Record<string, string[]> = {
        USER_NAME: profileForm.USER_NAME ? [profileForm.USER_NAME] : [],
        USER_ALIAS: profileForm.USER_ALIAS ? [profileForm.USER_ALIAS] : [],
        USER_EMAIL: profileForm.USER_EMAIL ? [profileForm.USER_EMAIL] : [],
        USER_TEAM: profileForm.TEAM_NAME ? [profileForm.TEAM_NAME] : [],
        MANAGER_INFO: profileForm.MANAGER_NAME ? [profileForm.MANAGER_NAME] : [],
        USER_COMPANY: profileForm.ORG_NAME ? [profileForm.ORG_NAME] : [],
      };
      await api.saveUserProfile(payload);
    } catch (err: any) {
      setProfileError(err.message || 'Failed to save profile');
    } finally {
      setProfileSaving(false);
    }
  }, [profileForm]);

  /* ---- Topics ---- */
  const [topicsLoading, setTopicsLoading] = useState(false);
  const [topicsError, setTopicsError] = useState<string | null>(null);
  const [following, setFollowing] = useState<string[]>([]);
  const [notFollowing, setNotFollowing] = useState<string[]>([]);
  const [baseTopicKeys, setBaseTopicKeys] = useState<Set<string>>(new Set());
  const [topicsLoaded, setTopicsLoaded] = useState(false);

  const _updateBaseKeys = (topics: any) => {
    const baseFollowing: string[] = (topics?.following ?? []);
    setBaseTopicKeys(new Set(baseFollowing.map((s: string) => s.trim().toLowerCase())));
  };

  const loadTopics = useCallback(async () => {
    if (topicsLoaded) return;
    setTopicsLoading(true);
    setTopicsError(null);
    try {
      const data = await api.getTopicsState();
      setFollowing(data.following ?? []);
      setNotFollowing(data.not_following ?? []);
      _updateBaseKeys(data.topics);
      setTopicsLoaded(true);
    } catch (err: any) {
      setTopicsError(err.message || 'Failed to load topics');
    } finally {
      setTopicsLoading(false);
    }
  }, [topicsLoaded]);

  const toggleTopic = useCallback(async (topic: string, currentlyFollowing: boolean) => {
    setTopicsLoading(true);
    setTopicsError(null);
    try {
      const target = currentlyFollowing ? 'not_following' : 'following';
      const data = await api.setTopicTarget(topic, target);
      setFollowing(data.following ?? []);
      setNotFollowing(data.not_following ?? []);
      _updateBaseKeys(data.topics);
    } catch (err: any) {
      setTopicsError(err.message || 'Failed to update topic');
    } finally {
      setTopicsLoading(false);
    }
  }, []);

  /* ---- Pipeline Config ---- */
  const [configLoading, setConfigLoading] = useState(false);
  const [configSaving, setConfigSaving] = useState(false);
  const [configError, setConfigError] = useState<string | null>(null);
  const [pipelineConfig, setPipelineConfig] = useState<Record<string, any> | null>(null);
  const [configLoaded, setConfigLoaded] = useState(false);

  // AI provider / model state
  const [aiBackend, setAiBackend] = useState<'azure' | 'copilot'>('azure');
  const [copilotModel, setCopilotModel] = useState(DEFAULT_COPILOT_MODEL);
  const [azureModel, setAzureModel] = useState('gpt-5.1');
  const [modelOptions, setModelOptions] = useState<{ azure: string[]; copilot: string[] }>({ azure: [], copilot: [] });
  const [copilotLoggedIn, setCopilotLoggedIn] = useState<boolean | null>(null);
  const [azureLoggedIn, setAzureLoggedIn] = useState<boolean | null>(null);
  const [copilotDeviceFlow, setCopilotDeviceFlow] = useState<{
    user_code: string;
    verification_uri: string;
  } | null>(null);
  const [copilotLoginError, setCopilotLoginError] = useState('');
  const [copilotLoginLoading, setCopilotLoginLoading] = useState(false);
  const copilotAbortRef = useRef<AbortController | null>(null);
  const [pingResult, setPingResult] = useState<{ ok: boolean; reply?: string; model?: string; elapsed_s?: number; error?: string } | null>(null);
  const [pingLoading, setPingLoading] = useState(false);

  const refreshAiModels = useCallback(async (provider?: 'azure' | 'copilot') => {
    try {
      const models = await api.getAiModels(provider);
      setModelOptions((prev) => ({
        azure: provider === 'copilot' ? prev.azure : models.azure,
        copilot: prioritizeDefaultModel(
          provider === 'azure' ? prev.copilot : models.copilot,
          DEFAULT_COPILOT_MODEL,
        ),
      }));
    } catch {
      // ignore
    }
  }, []);

  // Fetch model lists once
  useEffect(() => {
    refreshAiModels();
  }, [refreshAiModels]);

  // Check Azure CLI status when azure backend is selected
  useEffect(() => {
    if (aiBackend !== 'azure') return;
    (async () => {
      try {
        const res = await api.getAzureStatus();
        setAzureLoggedIn(!!res?.logged_in);
      } catch {
        setAzureLoggedIn(false);
      }
    })();
  }, [aiBackend]);

  // Check Copilot status when backend switches to copilot
  useEffect(() => {
    if (aiBackend !== 'copilot') return;
    (async () => {
      try {
        const res = await api.getCopilotStatus();
        setCopilotLoggedIn(!!res?.logged_in);
        if (res?.logged_in) {
          refreshAiModels('copilot');
        }
      } catch {
        setCopilotLoggedIn(false);
      }
    })();
  }, [aiBackend, refreshAiModels]);

  const handleCopilotLogin = useCallback(async () => {
    setCopilotLoginLoading(true);
    setCopilotLoginError('');
    setCopilotDeviceFlow(null);
    const ac = new AbortController();
    copilotAbortRef.current = ac;
    try {
      const login = await api.copilotLogin();
      if (ac.signal.aborted) return;
      setCopilotDeviceFlow({
        user_code: login.user_code,
        verification_uri: login.verification_uri,
      });
      const result = await api.copilotLoginWait(login.device_code, login.interval, login.expires_in, ac.signal);
      setCopilotDeviceFlow(null);
      if (result.status === 'complete') {
        setCopilotLoggedIn(true);
        await refreshAiModels('copilot');
        setCopilotModel((prev) => prev || DEFAULT_COPILOT_MODEL);
      } else {
        setCopilotLoginError(result.error || 'Login failed');
      }
    } catch (e: any) {
      if (ac.signal.aborted) return;
      setCopilotDeviceFlow(null);
      setCopilotLoginError(e?.message || 'Failed to start login');
    } finally {
      copilotAbortRef.current = null;
      setCopilotLoginLoading(false);
    }
  }, []);

  const handleCopilotCancel = useCallback(() => {
    copilotAbortRef.current?.abort();
    setCopilotDeviceFlow(null);
    setCopilotLoginLoading(false);
  }, []);

  const handleAiPing = useCallback(async () => {
    setPingLoading(true);
    setPingResult(null);
    try {
      const model = aiBackend === 'copilot' ? copilotModel : azureModel;
      const res = await api.aiPing(aiBackend, model);
      setPingResult(res);
    } catch (e: any) {
      setPingResult({ ok: false, error: e?.message || 'Request failed' });
    } finally {
      setPingLoading(false);
    }
  }, [aiBackend, copilotModel, azureModel]);

  const DISPLAY_KEYS = useMemo(() => [
    'run_interval_minutes',
    'max_emails_per_run',
    'max_teams_chats_per_run',
    'outlook_parallel_workers',
    'obsolete_days_threshold',
  ], []);

  const FRIENDLY_LABELS: Record<string, string> = useMemo(() => ({
    run_interval_minutes: 'Run Interval (min)',
    max_emails_per_run: 'Max Emails / Run',
    max_teams_chats_per_run: 'Max Teams Chats / Run',
    outlook_parallel_workers: 'Concurrency Workers',
    obsolete_days_threshold: 'Obsolete Days Threshold',
  }), []);

  const loadConfig = useCallback(async () => {
    if (configLoaded) return;
    setConfigLoading(true);
    setConfigError(null);
    try {
      const { config } = await api.getPipelineConfig();
      setPipelineConfig(config || {});
      // Sync AI backend / model from loaded config
      const backend = (config?.ai_backend || '').toString().toLowerCase();
      if (backend === 'copilot' || backend === 'azure') setAiBackend(backend);
      const cModel = (config?.copilot_model || '').toString().trim();
      if (cModel) setCopilotModel(cModel);
      const aModel = (config?.azure_model || '').toString().trim();
      if (aModel) setAzureModel(aModel);
      setConfigLoaded(true);
    } catch (err: any) {
      setConfigError(err.message || 'Failed to load config');
    } finally {
      setConfigLoading(false);
    }
  }, [configLoaded]);

  const saveConfig = useCallback(async () => {
    if (!pipelineConfig) return;
    setConfigSaving(true);
    setConfigError(null);
    try {
      const updates: Record<string, any> = {};
      for (const key of DISPLAY_KEYS) {
        if (key in pipelineConfig) updates[key] = pipelineConfig[key];
      }
      // Include AI backend + model
      updates.ai_backend = aiBackend;
      updates.copilot_model = copilotModel;
      updates.azure_model = azureModel;
      const { config } = await api.savePipelineConfig(updates);
      setPipelineConfig(config || {});
    } catch (err: any) {
      setConfigError(err.message || 'Failed to save config');
    } finally {
      setConfigSaving(false);
    }
  }, [pipelineConfig, DISPLAY_KEYS, aiBackend, copilotModel, azureModel]);

  // Auto-save flash indicator
  const [aiSavedFlash, setAiSavedFlash] = useState(false);
  const aiSavedTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null);

  const saveAiConfig = useCallback(async () => {
    setConfigSaving(true);
    setConfigError(null);
    try {
      const updates: Record<string, any> = {
        ai_backend: aiBackend,
        copilot_model: copilotModel,
        azure_model: azureModel,
      };
      const { config } = await api.savePipelineConfig(updates);
      setPipelineConfig((prev) => ({ ...(prev || {}), ...config }));
      const activeModel = aiBackend === 'copilot' ? copilotModel : azureModel;
      onAiConfigChange?.(aiBackend, activeModel);
      // Flash "Saved" indicator
      setAiSavedFlash(true);
      if (aiSavedTimerRef.current) clearTimeout(aiSavedTimerRef.current);
      aiSavedTimerRef.current = setTimeout(() => setAiSavedFlash(false), 1800);
    } catch (err: any) {
      setConfigError(err.message || 'Failed to save AI config');
    } finally {
      setConfigSaving(false);
    }
  }, [aiBackend, copilotModel, azureModel, onAiConfigChange]);

  // Auto-save AI config when provider or model changes
  const aiConfigInitRef = useRef(false);
  useEffect(() => {
    if (!configLoaded) return;
    // Skip the first render (initial load from server)
    if (!aiConfigInitRef.current) {
      aiConfigInitRef.current = true;
      return;
    }
    saveAiConfig();
  }, [aiBackend, copilotModel, azureModel]); // eslint-disable-line react-hooks/exhaustive-deps

  /* ---- Data / Reset ---- */
  const [resetBusy, setResetBusy] = useState(false);
  const [resetResult, setResetResult] = useState<{ type: 'success' | 'error'; message: string } | null>(null);

  const handleResetClean = useCallback(async () => {
    if (!window.confirm('This will stop the pipeline and DELETE all fetched data. You will need to run the pipeline again from scratch.\n\nAre you sure?')) return;
    setResetBusy(true);
    setResetResult(null);
    try {
      await api.resetPipeline();
      setResetResult({ type: 'success', message: 'Pipeline reset complete. Restarting onboarding…' });
      onPipelineAction?.();
      // Trigger onboarding wizard after full reset
      onDataReset?.();
    } catch (err: any) {
      setResetResult({ type: 'error', message: err.message || 'Reset failed' });
    } finally {
      setResetBusy(false);
    }
  }, [onPipelineAction, onDataReset]);

  /* ---- Auto-load on tab switch ---- */
  useEffect(() => {
    if (!open) return;
    if (tab === 'profile') loadProfile();
    else if (tab === 'topics') loadTopics();
    else if (tab === 'ai' || tab === 'config') loadConfig();
  }, [open, tab, loadProfile, loadTopics, loadConfig]);

  /* ---- Close on Escape ---- */
  useEffect(() => {
    if (!open) return;
    const handler = (e: KeyboardEvent) => {
      if (e.key === 'Escape') onClose();
    };
    document.addEventListener('keydown', handler);
    return () => document.removeEventListener('keydown', handler);
  }, [open, onClose]);

  if (!open) return null;

  /* ---- Render helpers ---- */
  const renderProfileTab = () => (
    <div className="v2-settings-section">
      {profileLoading && !profileLoaded && <div className="v2-settings-loader">Loading…</div>}
      {profileError && <div className="v2-settings-error">{profileError}</div>}
      {profileLoaded && (
        <>
          {(['USER_NAME', 'USER_ALIAS', 'USER_EMAIL', 'TEAM_NAME', 'MANAGER_NAME', 'ORG_NAME'] as const).map((key) => (
            <label key={key} className="v2-settings-field">
              <span className="v2-settings-field-label">
                {key.replace(/_/g, ' ').replace(/\b\w/g, (c) => c.toUpperCase())}
              </span>
              <input
                type="text"
                value={profileForm[key] ?? ''}
                onChange={(e) => setProfileForm((prev) => ({ ...prev, [key]: e.target.value }))}
                disabled={profileSaving}
              />
            </label>
          ))}
          <button
            className="v2-settings-save-btn"
            onClick={saveProfile}
            disabled={profileSaving}
          >
            {profileSaving ? 'Saving…' : 'Save Profile'}
          </button>
        </>
      )}
    </div>
  );

  const renderTopicsTab = () => (
    <div className="v2-settings-section">
      {topicsLoading && !topicsLoaded && <div className="v2-settings-loader">Loading…</div>}
      {topicsError && <div className="v2-settings-error">{topicsError}</div>}
      {topicsLoaded && (() => {
          const baseTopics = following.filter((t) => baseTopicKeys.has(t.trim().toLowerCase()));
          const focusTopics = following.filter((t) => !baseTopicKeys.has(t.trim().toLowerCase()));
          return (
            <>
              {baseTopics.length > 0 && (
                <div className="v2-settings-topics-group">
                  <h4 className="v2-settings-topic-heading">Topics</h4>
                  <div className="v2-settings-topic-chips">
                    {baseTopics.map((t) => (
                      <button
                        key={t}
                        className="v2-settings-topic-chip following"
                        onClick={() => toggleTopic(t, true)}
                        disabled={topicsLoading}
                        title="Click to unfollow"
                      >
                        {t} ✕
                      </button>
                    ))}
                  </div>
                </div>
              )}
              {focusTopics.length > 0 && (
                <div className="v2-settings-topics-group">
                  <h4 className="v2-settings-topic-heading">Recent Focus</h4>
                  <div className="v2-settings-topic-chips">
                    {focusTopics.map((t) => (
                      <button
                        key={t}
                        className="v2-settings-topic-chip focus"
                        onClick={() => toggleTopic(t, true)}
                        disabled={topicsLoading}
                        title="Click to unfollow"
                      >
                        {t} ✕
                      </button>
                    ))}
                  </div>
                </div>
              )}
          {notFollowing.length > 0 && (
            <div className="v2-settings-topics-group">
              <h4 className="v2-settings-topic-heading">Not Following</h4>
              <div className="v2-settings-topic-chips">
                {notFollowing.map((t) => (
                  <button
                    key={t}
                    className="v2-settings-topic-chip not-following"
                    onClick={() => toggleTopic(t, false)}
                    disabled={topicsLoading}
                    title="Click to follow"
                  >
                    + {t}
                  </button>
                ))}
              </div>
            </div>
          )}
              {following.length === 0 && notFollowing.length === 0 && (
                <div className="v2-settings-empty">No topics configured. Run the pipeline first to discover topics.</div>
              )}
            </>
          );
        })()}

      {/* ---- Banned Event Types ---- */}
      {allEventTypes && allEventTypes.length > 0 && onBannedTypesChange && (
        <div className="v2-settings-topics-group" style={{ marginTop: 18 }}>
          <h4 className="v2-settings-topic-heading">Banned Types</h4>
          <p className="v2-settings-banned-desc">Events of banned types are hidden from the card stream.</p>
          <div className="v2-settings-topic-chips">
            {allEventTypes.map((t) => {
              const isBanned = bannedTypes?.has(t) ?? false;
              return (
                <button
                  key={t}
                  className={`v2-settings-topic-chip ${isBanned ? 'banned' : 'not-banned'}`}
                  onClick={() => {
                    const next = new Set(bannedTypes);
                    if (isBanned) next.delete(t); else next.add(t);
                    onBannedTypesChange(next);
                  }}
                  title={isBanned ? 'Click to unban' : 'Click to ban'}
                >
                  {isBanned ? '🚫 ' : ''}{t}{isBanned ? '' : ' ✕'}
                </button>
              );
            })}
          </div>
        </div>
      )}
    </div>
  );

  const renderAiTab = () => (
    <div className="v2-settings-section">
      {configLoading && !configLoaded && <div className="v2-settings-loader">Loading…</div>}
      {configError && <div className="v2-settings-error">{configError}</div>}
      {configLoaded && (
        <>
          {/* ---- Provider toggle ---- */}
          <div className="v2-settings-field">
            <span className="v2-settings-field-label">AI Provider</span>
            <div className="v2-ai-backend-selector">
              <button
                className={`v2-ai-backend-btn ${aiBackend === 'azure' ? 'active' : ''}`}
                onClick={() => setAiBackend('azure')}
                type="button"
              >
                ☁️ Azure OpenAI
              </button>
              <button
                className={`v2-ai-backend-btn ${aiBackend === 'copilot' ? 'active' : ''}`}
                onClick={() => setAiBackend('copilot')}
                type="button"
              >
                🐙 GitHub Copilot
              </button>
            </div>
          </div>

          {/* ---- Model selector ---- */}
          <label className="v2-settings-field">
            <span className="v2-settings-field-label">Model</span>
            <select
              className="v2-settings-select"
              value={aiBackend === 'copilot' ? copilotModel : azureModel}
              onChange={(e) => {
                if (aiBackend === 'copilot') setCopilotModel(e.target.value);
                else setAzureModel(e.target.value);
              }}
            >
              {(aiBackend === 'copilot' ? modelOptions.copilot : modelOptions.azure).map((m) => (
                <option key={m} value={m}>{formatModelOptionLabel(m, aiBackend)}</option>
              ))}
            </select>
          </label>

          {/* Auto-save indicator */}
          <div className={`v2-ai-autosave-indicator ${aiSavedFlash ? 'visible' : ''} ${configSaving ? 'saving' : ''}`}>
            {configSaving ? 'Saving…' : '✓ Saved'}
          </div>

          {/* ---- Connection status ---- */}
          <div className="v2-settings-field">
            <span className="v2-settings-field-label">Connection Status</span>
            {aiBackend === 'azure' && (
              <div className={`v2-ai-status-badge ${azureLoggedIn ? 'ok' : azureLoggedIn === false ? 'err' : 'loading'}`}>
                {azureLoggedIn === null && '⏳ Checking Azure CLI…'}
                {azureLoggedIn === true && '✓ Azure CLI logged in'}
                {azureLoggedIn === false && '✗ Azure CLI not logged in — run az login'}
              </div>
            )}
            {aiBackend === 'copilot' && (
              <div className={`v2-ai-status-badge ${copilotLoggedIn ? 'ok' : copilotLoggedIn === false ? 'err' : 'loading'}`}>
                {copilotLoggedIn === null && '⏳ Checking Copilot…'}
                {copilotLoggedIn === true && '✓ GitHub Copilot connected'}
                {copilotLoggedIn === false && !copilotLoginLoading && (
                  <>
                    ✗ Not connected
                    <button className="v2-btn-inline" onClick={handleCopilotLogin} type="button">
                      Sign in
                    </button>
                  </>
                )}
                {copilotLoginLoading && !copilotDeviceFlow && '⏳ Starting login…'}
              </div>
            )}
          </div>

          {/* Copilot device flow */}
          {copilotDeviceFlow && (
            <div className="v2-copilot-device-flow">
              <p>
                Enter this code at{' '}
                <a href={copilotDeviceFlow.verification_uri} target="_blank" rel="noopener noreferrer">
                  {copilotDeviceFlow.verification_uri}
                </a>:
              </p>
              <span className="v2-copilot-code">{copilotDeviceFlow.user_code}</span>
              <button className="v2-btn-inline" onClick={handleCopilotCancel} type="button">Cancel</button>
            </div>
          )}
          {copilotLoginError && (
            <div className="v2-settings-error" style={{ marginTop: 0 }}>{copilotLoginError}</div>
          )}

          {/* ---- Test AI Connection ---- */}
          <div className="v2-settings-field">
            <span className="v2-settings-field-label">Test</span>
            <div className="v2-settings-ai-ping">
              <button
                className="v2-ai-test-btn"
                onClick={handleAiPing}
                disabled={pingLoading}
                type="button"
              >
                {pingLoading ? '⏳ Testing…' : '⚡ Test Connection'}
              </button>
              {pingResult && (
                <div className={`v2-ai-ping-result ${pingResult.ok ? 'ok' : 'fail'}`}>
                  {pingResult.ok
                    ? `✓ ${pingResult.reply} (${pingResult.model}, ${pingResult.elapsed_s?.toFixed(2)}s)`
                    : `✗ ${pingResult.error}`}
                </div>
              )}
            </div>
          </div>
        </>
      )}
    </div>
  );

  const renderConfigTab = () => (
    <div className="v2-settings-section">
      {configLoading && !configLoaded && <div className="v2-settings-loader">Loading…</div>}
      {configError && <div className="v2-settings-error">{configError}</div>}
      {configLoaded && pipelineConfig && (
        <>
          {DISPLAY_KEYS.filter((k) => k in pipelineConfig).map((key) => {
            const val = pipelineConfig[key];
            const isBool = typeof val === 'boolean';
            return (
              <label key={key} className="v2-settings-field">
                <span className="v2-settings-field-label">{FRIENDLY_LABELS[key] || key}</span>
                {isBool ? (
                  <div className="v2-settings-toggle-row">
                    <button
                      className={`v2-settings-toggle ${val ? 'on' : 'off'}`}
                      onClick={() => setPipelineConfig((prev) => ({ ...(prev || {}), [key]: !val }))}
                      disabled={configSaving}
                      type="button"
                    >
                      {val ? 'ON' : 'OFF'}
                    </button>
                  </div>
                ) : (
                  <input
                    type={typeof val === 'number' ? 'number' : 'text'}
                    value={val ?? ''}
                    {...(key === 'outlook_parallel_workers' ? { min: 1, max: 5, step: 1 } : {})}
                    onChange={(e) => {
                      let next: string | number = typeof val === 'number' ? Number(e.target.value) : e.target.value;
                      if (key === 'outlook_parallel_workers') next = Math.max(1, Math.min(5, Number(next) || 1));
                      setPipelineConfig((prev) => ({ ...(prev || {}), [key]: next }));
                    }}
                    disabled={configSaving}
                  />
                )}
              </label>
            );
          })}
          <button
            className="v2-settings-save-btn"
            onClick={saveConfig}
            disabled={configSaving}
          >
            {configSaving ? 'Saving…' : 'Save Config'}
          </button>
        </>
      )}
    </div>
  );

  return (
    <>
      <div className="v2-settings-backdrop" onClick={onClose} />
      <div className="v2-settings-drawer">
        <div className="v2-settings-drawer-header">
          <h2>Settings</h2>
          <button className="v2-settings-close" onClick={onClose} title="Close">✕</button>
        </div>

        <div className="v2-settings-tabs">
          {([
            ['profile', 'Profile'],
            ['topics', 'Topics'],
            ['ai', 'AI'],
            ['config', 'Pipeline'],
            ['data', 'Data'],
          ] as [Tab, string][]).map(([key, label]) => (
            <button
              key={key}
              className={`v2-settings-tab ${tab === key ? 'active' : ''}`}
              onClick={() => setTab(key)}
            >
              {label}
            </button>
          ))}
        </div>

        <div className="v2-settings-body">
          {tab === 'profile' && renderProfileTab()}
          {tab === 'topics' && renderTopicsTab()}
          {tab === 'ai' && renderAiTab()}
          {tab === 'config' && renderConfigTab()}
          {tab === 'data' && (
            <div className="v2-settings-section">
              {resetResult && (
                <div className={resetResult.type === 'success' ? 'v2-settings-success' : 'v2-settings-error'}>
                  {resetResult.message}
                </div>
              )}

              <div className="v2-settings-data-card danger">
                <h4>Full Reset</h4>
                <p className="v2-settings-data-desc">
                  Stop the pipeline and <strong>delete all data</strong> (emails, messages, extracted actions). You will need to run the pipeline again from scratch.
                </p>
                <button
                  className="v2-settings-data-btn danger"
                  onClick={handleResetClean}
                  disabled={resetBusy}
                >
                  {resetBusy ? 'Working…' : 'Reset &amp; Delete All Data'}
                </button>
              </div>
            </div>
          )}
        </div>
      </div>
    </>
  );
};
