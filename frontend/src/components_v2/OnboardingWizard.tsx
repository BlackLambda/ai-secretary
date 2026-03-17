/**
 * OnboardingWizard — Full-page 3-step wizard for new users (v2 UI).
 *
 * Steps:
 *   1. Detect user basic info (fetch profile + az login check)
 *   2. Recent focus (AI-analyzed topics to follow/unfollow)
 *   3. Start pipeline
 */
import React, { useState, useEffect, useCallback, useRef } from 'react';
import { api } from '../api';
import './OnboardingWizard.css';

interface Props {
  onComplete: () => void;
}

type Step = 1 | 2 | 3;

type FocusTopic = {
  name?: string;
  topic?: string;
  score?: number;
  rationale?: string;
  keywords?: string[];
  followed?: boolean;   // local toggle state
};

const STORAGE_KEY = 'ai_secretary.onboarding';

function loadSaved(): {
  step?: Step;
  azureLoggedIn?: boolean;
  profile?: Record<string, any>;
  focusSummary?: string;
  focusTopics?: FocusTopic[];
  fetchDaysBack?: number;
  aiBackend?: 'azure' | 'copilot';
  azureModel?: string;
  copilotModel?: string;
} {
  try {
    const raw = localStorage.getItem(STORAGE_KEY);
    if (raw) return JSON.parse(raw);
  } catch { /* ignore */ }
  return {};
}

function saveDraft(patch: Record<string, any>) {
  try {
    const prev = loadSaved();
    localStorage.setItem(STORAGE_KEY, JSON.stringify({ ...prev, ...patch }));
  } catch { /* ignore */ }
}

export const OnboardingWizard: React.FC<Props> = ({ onComplete }) => {
  const saved = React.useRef(loadSaved()).current;

  const [step, setStep] = useState<Step>(saved.step ?? 1);

  // Step 1 state
  const [azureLoggedIn, setAzureLoggedIn] = useState<boolean | null>(saved.azureLoggedIn ?? null);
  const [azureError, setAzureError] = useState('');
  const [azureLoginLoading, setAzureLoginLoading] = useState(false);
  const [azureLoginError, setAzureLoginError] = useState('');
  const [profileLoading, setProfileLoading] = useState(false);
  const [profile, setProfile] = useState<Record<string, any> | null>(saved.profile ?? null);
  const [profileError, setProfileError] = useState('');

  // AI backend state
  const [aiBackend, setAiBackend] = useState<'azure' | 'copilot'>(saved.aiBackend ?? 'copilot');
  const [copilotLoggedIn, setCopilotLoggedIn] = useState<boolean | null>(null);
  const [copilotDeviceFlow, setCopilotDeviceFlow] = useState<{
    user_code: string;
    verification_uri: string;
  } | null>(null);
  const [copilotLoginError, setCopilotLoginError] = useState('');
  const [copilotLoginLoading, setCopilotLoginLoading] = useState(false);
  const copilotAbortRef = useRef<AbortController | null>(null);

  // Ping state
  const [pingResult, setPingResult] = useState<{ ok: boolean; reply?: string; model?: string; elapsed_s?: number; error?: string } | null>(null);
  const [pinging, setPinging] = useState(false);

  // Model selection state
  const [modelOptions, setModelOptions] = useState<{ azure: string[]; copilot: string[] }>({ azure: [], copilot: [] });
  const [modelLoading, setModelLoading] = useState<{ azure: boolean; copilot: boolean }>({ azure: false, copilot: false });
  const [modelLoadError, setModelLoadError] = useState('');
  const [azureModel, setAzureModel] = useState<string>(saved.azureModel ?? '');
  const [copilotModel, setCopilotModel] = useState<string>(saved.copilotModel ?? '');

  // Step 2 state
  const [focusLoading, setFocusLoading] = useState(false);
  const [focusSummary, setFocusSummary] = useState(saved.focusSummary ?? '');
  const [focusTopics, setFocusTopics] = useState<FocusTopic[]>(saved.focusTopics ?? []);
  const [focusError, setFocusError] = useState('');

  // Step 1 Next button validation state
  const [nextChecking, setNextChecking] = useState(false);
  const [nextError, setNextError] = useState('');

  // Persist key state changes to localStorage
  useEffect(() => { saveDraft({ step }); }, [step]);
  useEffect(() => { if (profile) saveDraft({ profile }); }, [profile]);
  useEffect(() => { if (azureLoggedIn !== null) saveDraft({ azureLoggedIn }); }, [azureLoggedIn]);
  useEffect(() => { if (focusSummary) saveDraft({ focusSummary }); }, [focusSummary]);
  useEffect(() => { if (focusTopics.length > 0) saveDraft({ focusTopics }); }, [focusTopics]);
  useEffect(() => { saveDraft({ aiBackend }); }, [aiBackend]);
  useEffect(() => { saveDraft({ azureModel }); }, [azureModel]);
  useEffect(() => { saveDraft({ copilotModel }); }, [copilotModel]);

  // Fetch model lists lazily for the selected backend so the dropdown appears faster.
  useEffect(() => {
    if (modelOptions[aiBackend].length > 0 || modelLoading[aiBackend]) {
      return;
    }
    let cancelled = false;
    setModelLoadError('');
    setModelLoading((prev) => ({ ...prev, [aiBackend]: true }));
    api.getAiModels(aiBackend)
      .then((res) => {
        if (cancelled) return;
        setModelOptions((prev) => ({
          azure: aiBackend === 'azure' ? (Array.isArray(res.azure) ? res.azure : []) : prev.azure,
          copilot: aiBackend === 'copilot' ? (Array.isArray(res.copilot) ? res.copilot : []) : prev.copilot,
        }));
      })
      .catch(() => {
        if (cancelled) return;
        setModelLoadError('Failed to load models. Please try again.');
      })
      .finally(() => {
        if (cancelled) return;
        setModelLoading((prev) => ({ ...prev, [aiBackend]: false }));
      });
    return () => {
      cancelled = true;
    };
  }, [aiBackend, modelLoading, modelOptions]);

  useEffect(() => {
    if (azureModel && modelOptions.azure.length > 0 && !modelOptions.azure.includes(azureModel)) {
      setAzureModel('');
      setPingResult(null);
    }
  }, [azureModel, modelOptions.azure]);

  useEffect(() => {
    if (copilotModel && modelOptions.copilot.length > 0 && !modelOptions.copilot.includes(copilotModel)) {
      setCopilotModel('');
      setPingResult(null);
    }
  }, [copilotModel, modelOptions.copilot]);

  // Check Copilot login status when copilot backend is selected
  useEffect(() => {
    if (aiBackend !== 'copilot') return;
    (async () => {
      try {
        const res = await api.getCopilotStatus();
        setCopilotLoggedIn(!!res?.logged_in);
      } catch {
        setCopilotLoggedIn(false);
      }
    })();
  }, [aiBackend]);

  const handleCopilotLogin = useCallback(async () => {
    setCopilotLoginLoading(true);
    setCopilotLoginError('');
    setCopilotDeviceFlow(null);
    const ac = new AbortController();
    copilotAbortRef.current = ac;
    try {
      // Step 1: Get device code (instant)
      const login = await api.copilotLogin();
      if (ac.signal.aborted) return;
      setCopilotDeviceFlow({
        user_code: login.user_code,
        verification_uri: login.verification_uri,
      });

      // Step 2: Server-side long-poll — blocks until user authorizes or timeout
      const result = await api.copilotLoginWait(login.device_code, login.interval, login.expires_in, ac.signal);
      setCopilotDeviceFlow(null);
      if (result.status === 'complete') {
        setCopilotLoggedIn(true);
      } else {
        setCopilotLoginError(result.error || 'Login failed');
      }
    } catch (e: any) {
      if (ac.signal.aborted) return; // user cancelled
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

  // Step 2 progress tracking — poll real backend progress
  const [focusProgress, setFocusProgress] = useState<{
    label: string;
    percent: number;
    step: number;
    total: number;
    done?: boolean;
  }>({ label: '', percent: 0, step: 0, total: 0, done: false });
  const [focusElapsed, setFocusElapsed] = useState(0);

  useEffect(() => {
    if (!focusLoading) { setFocusElapsed(0); setFocusProgress({ label: '', percent: 0, step: 0, total: 0, done: false }); return; }
    const t0 = Date.now();
    const iv = setInterval(async () => {
      setFocusElapsed(Math.floor((Date.now() - t0) / 1000));
      try {
        const prog = await api.getFocusAnalysisProgress();
        if (prog && prog.label) {
          setFocusProgress({
            label: prog.label,
            percent: prog.percent ?? 0,
            step: prog.step ?? 0,
            total: prog.total ?? 0,
            done: prog.status === 'done',
          });
        }
      } catch { /* ignore */ }
    }, 2000);
    return () => clearInterval(iv);
  }, [focusLoading]);

  // Step 3 state
  const [starting, setStarting] = useState(false);
  const [started, setStarted] = useState(false);
  const [fetchDaysBack, setFetchDaysBack] = useState<number>(saved.fetchDaysBack ?? 0.25);
  useEffect(() => { saveDraft({ fetchDaysBack }); }, [fetchDaysBack]);

  const FETCH_OPTIONS: { value: number; label: string; hint: string }[] = [
    { value: 0.25, label: '6 hours', hint: 'Recommended — fast first run, covers recent activity' },
    { value: 1, label: '1 day', hint: 'Fetches a full day of emails and chats — slower first run' },
  ];

  /* --------------------------------------------------------------- */
  /*  Step 1: Azure login check                                       */
  /* --------------------------------------------------------------- */

  useEffect(() => {
    // Always re-check Azure CLI status on mount (login state can change).
    (async () => {
      try {
        const res: any = await api.getAzureStatus();
        setAzureLoggedIn(!!res?.logged_in);
        if (res?.error) setAzureError(res.error);
        else setAzureError('');
      } catch {
        setAzureLoggedIn(false);
        setAzureError('Could not check Azure CLI status');
      }
    })();
  }, []);

  const handleFetchProfile = useCallback(async () => {
    if (azureLoggedIn !== true) {
      setProfileError('Microsoft profile detection requires a work-account sign-in first. Use Login with Azure, then fetch the profile.');
      return;
    }
    setProfileLoading(true);
    setProfileError('');
    try {
      const res: any = await api.refetchUserProfile();
      const p = res?.profile || {};
      setProfile(p);
    } catch (e: any) {
      setProfileError(e?.message || 'Failed to fetch profile');
    } finally {
      setProfileLoading(false);
    }
  }, [azureLoggedIn]);

  const selectedModel = aiBackend === 'copilot' ? copilotModel : azureModel;
  const selectedModelOptions = aiBackend === 'copilot' ? modelOptions.copilot : modelOptions.azure;
  const selectedModelLoading = aiBackend === 'copilot' ? modelLoading.copilot : modelLoading.azure;
  const selectedModelReady = !!selectedModel && selectedModelOptions.includes(selectedModel);

  // Auto-fetch profile only after Microsoft auth is ready.
  // Copilot connectivity alone is not sufficient for Substrate profile lookup.
  useEffect(() => {
    if (azureLoggedIn === true && !profile && !profileLoading) {
      handleFetchProfile();
    }
  }, [azureLoggedIn, profile, profileLoading, handleFetchProfile]);

  /* --------------------------------------------------------------- */
  /*  Step 2: Recent focus                                            */
  /* --------------------------------------------------------------- */

  const handleAnalyzeFocus = useCallback(async () => {
    setFocusLoading(true);
    setFocusError('');
    try {
      const res: any = await api.analyzeRecentFocus(7);
      const report = res?.report;
      const focus = report?.focus;
      setFocusSummary(String(focus?.summary ?? '').trim());
      const rawTopics: any[] = Array.isArray(focus?.topics) ? focus.topics : [];
      setFocusTopics(
        rawTopics.map((t) => ({
          name: t.name || t.topic || '',
          topic: t.topic || t.name || '',
          score: t.score,
          rationale: t.rationale,
          keywords: t.keywords,
          followed: true, // default all to followed
        })),
      );
    } catch (e: any) {
      setFocusError(e?.message || 'Failed to analyze recent focus');
    } finally {
      setFocusLoading(false);
    }
  }, []);

  // (No auto-trigger — user clicks Analyze to start)

  // On step-2 mount: if a prior analysis is still running in the background,
  // resume the progress display and load results when it finishes.
  useEffect(() => {
    if (step !== 2 || focusTopics.length > 0 || focusLoading) return;
    let cancelled = false;
    (async () => {
      try {
        const prog = await api.getFocusAnalysisProgress();
        if (cancelled || !prog || prog.status === 'idle' || prog.status === 'error') return;
        // Analysis is running (or just finished) — show progress
        setFocusLoading(true);
        const iv = window.setInterval(async () => {
          if (cancelled) { clearInterval(iv); return; }
          try {
            const p = await api.getFocusAnalysisProgress();
            if (!p || p.status !== 'running') {
              clearInterval(iv);
              if (cancelled) return;
              try {
                const res: any = await api.getRecentFocus();
                const focus = res?.report?.focus;
                if (focus) {
                  setFocusSummary(String(focus.summary ?? '').trim());
                  const rawTopics: any[] = Array.isArray(focus.topics) ? focus.topics : [];
                  setFocusTopics(rawTopics.map((t) => ({
                    name: t.name || t.topic || '',
                    topic: t.topic || t.name || '',
                    score: t.score,
                    rationale: t.rationale,
                    keywords: t.keywords,
                    followed: true,
                  })));
                }
              } catch { /* ignore */ }
              if (!cancelled) setFocusLoading(false);
            }
          } catch { clearInterval(iv); if (!cancelled) setFocusLoading(false); }
        }, 2000);
      } catch { /* ignore */ }
    })();
    return () => { cancelled = true; };
  }, [step]); // eslint-disable-line react-hooks/exhaustive-deps

  const toggleTopic = (idx: number) => {
    setFocusTopics((prev) =>
      prev.map((t, i) => (i === idx ? { ...t, followed: !t.followed } : t)),
    );
  };

  /* --------------------------------------------------------------- */
  /*  Step 3: Start pipeline                                          */
  /* --------------------------------------------------------------- */

  const handleStart = useCallback(async () => {
    setStarting(true);
    try {
      // Save followed topics to profile first — replace (not append) to avoid
      // accumulating stale focus topics from previous runs.
      const followed = focusTopics.filter((t) => t.followed).map((t) => t.name || t.topic || '').filter(Boolean);
      if (followed.length > 0) {
        try {
          await api.replaceFocusTopics(followed);
        } catch {
          // Best-effort — don't block pipeline start
        }
      }

      // Save fetch_days_back, ai_backend, and model to pipeline config
      try {
        await api.savePipelineConfig({
          initial_fetch_days: fetchDaysBack,
          ai_backend: aiBackend,
          copilot_model: copilotModel,
          azure_model: azureModel,
        });
      } catch {
        // Best-effort
      }

      await api.startPipeline();
      setStarted(true);
      // Clear onboarding draft on success
      try { localStorage.removeItem(STORAGE_KEY); } catch { /* ignore */ }
      window.setTimeout(() => onComplete(), 1500);
    } catch (e: any) {
      window.alert(e?.message || 'Failed to start pipeline');
    } finally {
      setStarting(false);
    }
  }, [focusTopics, fetchDaysBack, aiBackend, copilotModel, azureModel, onComplete]);

  /* --------------------------------------------------------------- */
  /*  Render                                                          */
  /* --------------------------------------------------------------- */

  return (
    <div className="v2-onboarding">
      <div className="v2-onboarding-card">
        {/* Header */}
        <div className="v2-onboarding-header">
          <span className="v2-onboarding-logo">🤖</span>
          <h1>Welcome to AI Secretary</h1>
          <p className="v2-onboarding-intro">Let's set you up in a few quick steps.</p>
        </div>

        {/* Step indicators */}
        <div className="v2-onboarding-steps">
          {[1, 2, 3].map((s) => (
            <div key={s} className={`v2-step-dot ${step === s ? 'active' : ''} ${step > s ? 'done' : ''}`}>
              {step > s ? '✓' : s}
            </div>
          ))}
        </div>

        {/* Step content */}
        <div className="v2-onboarding-body">

          {/* ---- Step 1: Profile ---- */}
          {step === 1 && (
            <div className="v2-onboarding-step">
              <h2>Detect Your Info</h2>
              <p className="v2-step-desc">We'll fetch your profile from the corporate directory.</p>

              {/* AI Backend selector */}
              <div className="v2-ai-backend-section">
                <label className="v2-ai-backend-label">AI Backend</label>
                <div className="v2-ai-backend-selector">
                  <button
                    className={`v2-lookback-chip ${aiBackend === 'azure' ? 'active' : ''}`}
                    onClick={() => { setAiBackend('azure'); setPingResult(null); }}
                    title="Requires: Azure CLI installed and logged in (az login), plus access granted to the Azure OpenAI resource. Contact your Azure admin if you need to be added to the resource whitelist."
                  >
                    ☁️ Azure OpenAI
                  </button>
                  <button
                    className={`v2-lookback-chip ${aiBackend === 'copilot' ? 'active' : ''}`}
                    onClick={() => { setAiBackend('copilot'); setPingResult(null); }}
                    title="Requires: GitHub account with an active Copilot subscription (Individual or Business). No Azure account needed. You will be asked to sign in with GitHub below."
                  >
                    🐙 GitHub Copilot
                  </button>
                </div>
                {aiBackend === 'azure' && (
                  <p className="v2-onboarding-hint">
                    Requires <strong>Azure CLI</strong> installed and authenticated (<code>az login</code>), and your account must be granted access to the Azure OpenAI resource. Contact your Azure admin if you need to be added to the resource whitelist.
                  </p>
                )}
                {aiBackend === 'copilot' && (
                  <p className="v2-onboarding-hint">
                    Requires a <strong>GitHub account</strong> with an active <strong>Copilot subscription</strong> (Individual or Business). No Azure account needed. Sign in with GitHub below to connect.
                  </p>
                )}
              </div>

              {/* Model selector */}
              <div className="v2-ai-backend-section" style={{ display: 'flex', alignItems: 'center', gap: 12, flexWrap: 'wrap' }}>
                <label className="v2-ai-backend-label" style={{ marginRight: 4 }}>Model</label>
                <select
                  className="v2-onboarding-select"
                  value={selectedModel}
                  onChange={(e) => {
                    if (aiBackend === 'copilot') setCopilotModel(e.target.value);
                    else setAzureModel(e.target.value);
                    setPingResult(null);
                  }}
                  disabled={selectedModelLoading}
                >
                  <option value="">
                    {selectedModelLoading
                      ? 'Loading models…'
                      : selectedModelOptions.length > 0
                        ? 'Select a model'
                        : 'No models available'}
                  </option>
                  {selectedModelOptions.map((m) => (
                    <option key={m} value={m}>{m}</option>
                  ))}
                </select>
                <button
                  className="v2-btn v2-btn-secondary v2-btn-sm"
                  disabled={pinging || !selectedModelReady || (aiBackend === 'azure' ? azureLoggedIn !== true : copilotLoggedIn !== true)}
                  onClick={async () => {
                    setPinging(true);
                    setPingResult(null);
                    try {
                      const model = aiBackend === 'copilot' ? copilotModel : azureModel;
                      const res = await api.aiPing(aiBackend, model);
                      setPingResult(res);
                    } catch (e: any) {
                      setPingResult({ ok: false, error: e?.message || 'Ping failed' });
                    } finally {
                      setPinging(false);
                    }
                  }}
                >
                  {pinging ? '⏳ Testing…' : '🏓 Ping'}
                </button>
                {modelLoadError && <span style={{ fontSize: 13, color: '#dc2626' }}>{modelLoadError}</span>}
                {pingResult && (
                  <span style={{ fontSize: 13, color: pingResult.ok ? '#16a34a' : '#dc2626' }}>
                    {pingResult.ok
                      ? `✓ ${pingResult.model || 'OK'} (${pingResult.elapsed_s?.toFixed(1)}s)`
                      : `✗ ${pingResult.error || 'Failed'}`}
                  </span>
                )}
              </div>

              {/* Single status block below model — shows relevant backend status */}
              <div className={`v2-azure-status ${
                aiBackend === 'azure'
                  ? (azureLoggedIn === true ? 'ok' : azureLoggedIn === false ? 'err' : 'loading')
                  : (copilotLoggedIn === true ? 'ok' : copilotLoggedIn === false ? 'err' : 'loading')
              }`}>
                {aiBackend === 'azure' && (
                  <>
                    {azureLoggedIn === null && <span>Checking Azure CLI…</span>}
                    {azureLoggedIn === true && <span>✓ Azure CLI is logged in</span>}
                    {azureLoggedIn === false && (
                      <div className="v2-copilot-login-prompt">
                        <span>✗ Azure CLI not logged in</span>
                        <button
                          className="v2-btn v2-btn-primary v2-btn-sm"
                          onClick={async () => {
                            setAzureLoginLoading(true);
                            setAzureLoginError('');
                            try {
                              const res: any = await api.triggerAzureLogin();
                              setAzureLoggedIn(!!res?.logged_in);
                              if (res?.logged_in) {
                                setAzureError('');
                              } else {
                                setAzureLoginError(res?.error || 'Login failed. Please try again.');
                              }
                            } catch (e: any) {
                              setAzureLoginError(e?.message || 'Login failed. Please try again.');
                            } finally {
                              setAzureLoginLoading(false);
                            }
                          }}
                          disabled={azureLoginLoading}
                        >
                          {azureLoginLoading ? 'Opening browser…' : '🔑 Login with Azure'}
                        </button>
                        {(azureLoginError || azureError) && (
                          <span className="v2-azure-detail">{azureLoginError || azureError}</span>
                        )}
                      </div>
                    )}
                  </>
                )}
                {aiBackend === 'copilot' && (
                  <>
                    {copilotLoggedIn === null && <span>Checking Copilot credentials…</span>}
                    {copilotLoggedIn === true && <span>✓ GitHub Copilot connected</span>}
                    {copilotLoggedIn === true && azureLoggedIn !== true && (
                      <span className="v2-azure-detail">Profile detection still needs your Microsoft work account. Use Login with Azure before fetching your profile.</span>
                    )}
                    {copilotLoggedIn === false && !copilotDeviceFlow && (
                      <div className="v2-copilot-login-prompt">
                        <span>✗ GitHub Copilot not connected</span>
                        <button
                          className="v2-btn v2-btn-primary v2-btn-sm"
                          onClick={handleCopilotLogin}
                          disabled={copilotLoginLoading}
                        >
                          {copilotLoginLoading ? 'Starting…' : '🔑 Login with GitHub'}
                        </button>
                        {copilotLoginError && <span className="v2-azure-detail">{copilotLoginError}</span>}
                      </div>
                    )}
                    {copilotDeviceFlow && (
                      <div className="v2-copilot-device-flow">
                        <p>Open{' '}
                          <a href={copilotDeviceFlow.verification_uri} target="_blank" rel="noopener noreferrer">
                            {copilotDeviceFlow.verification_uri}
                          </a>{' '}and enter the code:
                        </p>
                        <div className="v2-copilot-code">{copilotDeviceFlow.user_code}</div>
                        <p className="v2-copilot-waiting">
                          Waiting for authorization…{' '}
                          <button className="v2-btn v2-btn-secondary v2-btn-inline" onClick={handleCopilotCancel}>Cancel</button>
                        </p>
                      </div>
                    )}
                  </>
                )}
              </div>

              {/* Profile card */}
              {profile && (
                <div className="v2-profile-card">
                  <div className="v2-profile-row"><span className="v2-profile-label">Name</span><span>{(Array.isArray(profile.USER_NAME) ? profile.USER_NAME[0] : profile.USER_NAME) || '—'}</span></div>
                  <div className="v2-profile-row"><span className="v2-profile-label">Alias</span><span>{(Array.isArray(profile.USER_ALIAS) ? profile.USER_ALIAS[0] : profile.USER_ALIAS) || '—'}</span></div>
                  <div className="v2-profile-row"><span className="v2-profile-label">Email</span><span>{(Array.isArray(profile.USER_EMAIL) ? profile.USER_EMAIL[0] : profile.USER_EMAIL) || '—'}</span></div>
                  {(() => { const v = Array.isArray(profile.USER_TEAM) ? profile.USER_TEAM[0] : profile.USER_TEAM; return v ? <div className="v2-profile-row"><span className="v2-profile-label">Team</span><span>{v}</span></div> : null; })()}
                  {(() => { const v = Array.isArray(profile.MANAGER_INFO) ? profile.MANAGER_INFO[0] : profile.MANAGER_INFO; return v ? <div className="v2-profile-row"><span className="v2-profile-label">Manager</span><span>{v}</span></div> : null; })()}
                  {(() => { const v = Array.isArray(profile.USER_JOB_TITLE) ? profile.USER_JOB_TITLE[0] : profile.USER_JOB_TITLE; return v ? <div className="v2-profile-row"><span className="v2-profile-label">Title</span><span>{v}</span></div> : null; })()}
                  {(() => { const v = Array.isArray(profile.USER_OFFICE_LOCATION) ? profile.USER_OFFICE_LOCATION[0] : profile.USER_OFFICE_LOCATION; return v ? <div className="v2-profile-row"><span className="v2-profile-label">Office</span><span>{v}</span></div> : null; })()}
                  {(() => { const v = Array.isArray(profile.USER_COMPANY) ? profile.USER_COMPANY[0] : profile.USER_COMPANY; return v ? <div className="v2-profile-row"><span className="v2-profile-label">Company</span><span>{v}</span></div> : null; })()}
                </div>
              )}

              {profileError && <div className="v2-step-error">{profileError}</div>}

              <div className="v2-step-actions">
                <button
                  className="v2-btn v2-btn-secondary"
                  onClick={handleFetchProfile}
                  disabled={profileLoading || azureLoggedIn !== true}
                >
                  {profileLoading ? 'Fetching…' : 'Fetch Profile'}
                </button>
                <button
                  className="v2-btn v2-btn-primary"
                  onClick={async () => {
                    setNextChecking(true);
                    setNextError('');
                    try {
                      if (!selectedModelReady) {
                        setNextError(selectedModelLoading ? 'Wait for the model list to finish loading, then choose a model.' : 'Choose a model before continuing.');
                        return;
                      }
                      // Double-check the selected AI provider connection before proceeding
                      if (aiBackend === 'azure') {
                        const res: any = await api.getAzureStatus();
                        setAzureLoggedIn(!!res?.logged_in);
                        if (!res?.logged_in) {
                          setNextError(res?.error || 'Azure CLI not logged in — run `az login` in a terminal first.');
                          return;
                        }
                      } else {
                        const res: any = await api.getCopilotStatus();
                        setCopilotLoggedIn(!!res?.logged_in);
                        if (!res?.logged_in) {
                          setNextError('GitHub Copilot not connected — please log in above first.');
                          return;
                        }
                      }
                      // Save ai_backend + model choice before moving to step 2 (focus analysis needs it)
                      try { await api.savePipelineConfig({ ai_backend: aiBackend, copilot_model: copilotModel, azure_model: azureModel }); } catch { /* best-effort */ }
                      setStep(2);
                    } catch (e: any) {
                      setNextError(e?.message || 'Failed to verify connection. Please try again.');
                    } finally {
                      setNextChecking(false);
                    }
                  }}
                  disabled={!profile || !selectedModelReady || selectedModelLoading || nextChecking}
                >
                  {nextChecking ? 'Checking…' : 'Next →'}
                </button>
                {nextError && (aiBackend === 'azure' ? azureLoggedIn : copilotLoggedIn) !== false && <div className="v2-step-error" style={{ marginTop: '0.5rem' }}>{nextError}</div>}
              </div>
            </div>
          )}

          {/* ---- Step 2: Focus ---- */}
          {step === 2 && (
            <div className="v2-onboarding-step">
              <h2>Your Recent Focus</h2>
              <p className="v2-step-desc">We analyzed your recent activity. Toggle topics you want to follow.</p>

              {focusLoading && (
                <div className="v2-focus-loading">
                  <div className="v2-loading-spinner" />
                  <div className="v2-focus-progress">
                    <span className="v2-focus-stage">
                      {focusProgress.done ? 'Saving results…' : (focusProgress.label || 'Initializing…')}
                      {!focusProgress.done && focusProgress.total > 0 && ` (${focusProgress.step}/${focusProgress.total})`}
                    </span>
                    <span className="v2-focus-elapsed">{Math.floor(focusElapsed / 60)}:{String(focusElapsed % 60).padStart(2, '0')} elapsed</span>
                    <div className="v2-focus-bar-track">
                      <div
                        className="v2-focus-bar-fill"
                        style={{ width: `${focusProgress.done ? 100 : (focusProgress.percent > 0 ? Math.min(99, focusProgress.percent) : Math.min(5, focusElapsed))}%` }}
                      />
                    </div>
                    <button
                      className="v2-btn v2-btn-secondary v2-btn-sm"
                      style={{ marginTop: '8px', alignSelf: 'center' }}
                      onClick={async () => {
                        await api.cancelRecentFocus();
                        setFocusLoading(false);
                        setFocusError('Analysis cancelled.');
                      }}
                    >
                      ✕ Stop
                    </button>
                  </div>
                </div>
              )}

              {focusSummary && <p className="v2-focus-summary">{focusSummary}</p>}

              {focusTopics.length > 0 && (
                <div className="v2-focus-topics">
                  {focusTopics.map((topic, i) => (
                    <button
                      key={i}
                      className={`v2-topic-pill ${topic.followed ? 'followed' : 'unfollowed'}`}
                      onClick={() => toggleTopic(i)}
                      title={topic.rationale || ''}
                    >
                      <span className="v2-topic-toggle">{topic.followed ? '✓' : '+'}</span>
                      <span className="v2-topic-name">{topic.name || topic.topic}</span>
                      {typeof topic.score === 'number' && (
                        <span className="v2-topic-score">{topic.score > 1 ? Math.round(topic.score) : Math.round(topic.score * 100)}%</span>
                      )}
                    </button>
                  ))}
                </div>
              )}

              {focusError && <div className="v2-step-error">{focusError}</div>}

              <div className="v2-step-actions">
                <button className="v2-btn v2-btn-secondary" onClick={() => setStep(1)}>← Back</button>
                <button
                  className="v2-btn v2-btn-secondary"
                  onClick={handleAnalyzeFocus}
                  disabled={focusLoading}
                >
                  {focusLoading ? 'Analyzing…' : (focusTopics.length === 0 ? 'Analyze' : 'Re-analyze')}
                </button>
                {focusTopics.length > 0 && (
                  <button
                    className="v2-btn v2-btn-secondary"
                    onClick={async () => {
                      if (!window.confirm('Clear the current recent focus data?')) return;
                      try { await api.clearRecentFocus(); } catch { /* best-effort */ }
                      setFocusTopics([]);
                      setFocusSummary('');
                      setFocusError('');
                      saveDraft({ focusTopics: [], focusSummary: '' });
                    }}
                    disabled={focusLoading}
                  >
                    Clear
                  </button>
                )}
                {focusTopics.length > 0 && (
                  <button className="v2-btn v2-btn-primary" onClick={() => setStep(3)}>
                    Next →
                  </button>
                )}
              </div>
            </div>
          )}

          {/* ---- Step 3: Launch ---- */}
          {step === 3 && (
            <div className="v2-onboarding-step">
              <h2>Ready to Go</h2>
              <p className="v2-step-desc">
                We'll fetch your latest Outlook emails and Teams messages, analyze them with AI,
                and build your daily briefing. This usually takes 2-5 minutes.
              </p>

              {profile && (
                <div className="v2-launch-summary">
                  <div>👤 <strong>{profile.USER_NAME || profile.USER_ALIAS}</strong></div>
                  {focusTopics.filter((t) => t.followed).length > 0 && (
                    <div className="v2-launch-topics">
                      <div className="v2-launch-topics-label">🎯 Following</div>
                      <div className="v2-launch-topics-list">
                        {focusTopics
                          .filter((t) => t.followed)
                          .map((t, i) => (
                            <span key={i} className="v2-launch-topic-chip">{t.name || t.topic}</span>
                          ))}
                      </div>
                    </div>
                  )}
                </div>
              )}

              {/* Lookback selector */}
              <div className="v2-lookback-row">
                <label className="v2-lookback-label" htmlFor="fetch-days">📅 Fetch emails & chats from the last</label>
                <div className="v2-lookback-selector">
                  {FETCH_OPTIONS.map((opt) => (
                    <button
                      key={opt.value}
                      className={`v2-lookback-chip ${fetchDaysBack === opt.value ? 'active' : ''}`}
                      onClick={() => setFetchDaysBack(opt.value)}
                      disabled={starting || started}
                    >
                      {opt.label}
                    </button>
                  ))}
                </div>
                <span className="v2-lookback-hint">
                  {(FETCH_OPTIONS.find(o => o.value === fetchDaysBack)?.hint) || 'Recommended — fast first run, covers recent activity'}
                </span>
              </div>

              {started && (
                <div className="v2-launch-success">
                  ✓ Pipeline started! Redirecting to your dashboard…
                </div>
              )}

              <div className="v2-step-actions">
                <button className="v2-btn v2-btn-secondary" onClick={() => setStep(2)} disabled={starting || started}>
                  ← Back
                </button>
                <button
                  className="v2-btn v2-btn-cta"
                  onClick={handleStart}
                  disabled={starting || started}
                >
                  {starting ? 'Starting…' : started ? 'Started ✓' : '🚀 Start Pipeline'}
                </button>
              </div>
            </div>
          )}
        </div>
      </div>
    </div>
  );
};
