/**
 * CompactHeader — Minimal top bar for the v2 UI.
 *
 * Layout:  [Logo/Title]  [subtitle + status dot]  [overflow actions]
 */
import React, { useState, useRef, useEffect } from 'react';
import './CompactHeader.css';
import { SchedulerPopover, ScheduleConfig } from './SchedulerPopover';

/* ---- Extension install guide modal ---- */
type ExtStep = 'choose' | 'confirm' | 'auto-running' | 'auto-done' | 'manual';

const ExtInstallGuide: React.FC<{ onClose: () => void }> = ({ onClose }) => {
  const backdropRef = useRef<HTMLDivElement>(null);
  const isEdge = /Edg\//i.test(navigator.userAgent);
  const browser = isEdge ? 'Edge' : 'Chrome';
  const extensionsUrl = isEdge ? 'edge://extensions' : 'chrome://extensions';

  const [step, setStep] = useState<ExtStep>('choose');
  const [autoOutput, setAutoOutput] = useState('');
  const [autoOk, setAutoOk] = useState<boolean | null>(null);
  const [countdown, setCountdown] = useState(3);
  const installingRef = useRef(false);

  useEffect(() => {
    if (step !== 'confirm') return;
    if (countdown <= 0) { startInstall(); return; }
    const t = setTimeout(() => setCountdown(c => c - 1), 1000);
    return () => clearTimeout(t);
  }, [step, countdown]);

  const startInstall = async () => {
    if (installingRef.current) return;
    installingRef.current = true;
    setStep('auto-running');
    setAutoOutput('');
    setAutoOk(null);
    const port = window.location.port || '5000';
    try {
      const res = await fetch('/api/install_extension', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ relaunch_url: `${window.location.protocol}//${window.location.hostname}:${port}` }),
      });
      const data = await res.json();
      setAutoOk(data.ok ?? false);
      setAutoOutput(data.output || data.error || '(no output)');
      setStep('auto-done');
    } catch (_err) {
      // Edge may be killed mid-install. Backend relaunches it.
    }
  };

  return (
    <div
      className="ext-guide-backdrop"
      ref={backdropRef}
      onClick={(e) => { if (e.target === backdropRef.current) onClose(); }}
    >
      <div className="ext-guide-modal">
        <button className="ext-guide-close" onClick={onClose} title="Close">✕</button>
        <h2 className="ext-guide-title">🧩 Install Browser Extension</h2>

        {step === 'choose' && (
          <>
            <p className="ext-guide-intro">
              The AI Secretary browser extension lets you view your prioritised tasks
              from the toolbar — without opening the full dashboard.
            </p>
            <p className="ext-guide-intro" style={{ marginTop: 4 }}>
              How would you like to install it?
            </p>
            <div className="ext-guide-choose-row">
              <button className="ext-guide-auto-btn" onClick={() => { setCountdown(3); installingRef.current = false; setStep('confirm'); }}>
                ⚡ Auto-install
                <span className="ext-guide-auto-hint">Runs the installer script automatically</span>
              </button>
              <button className="ext-guide-manual-btn" onClick={() => setStep('manual')}>
                🛠 Manual
                <span className="ext-guide-auto-hint">Step-by-step instructions</span>
              </button>
            </div>
          </>
        )}

        {step === 'confirm' && (
          <div className="ext-guide-confirm">
            <div className="ext-guide-confirm-icon">⚠️</div>
            <p className="ext-guide-confirm-msg">
              <strong>This browser window will close</strong> while the extension is
              installed, then reopen automatically to this page.
            </p>
            <div className="ext-guide-countdown">
              <span className="ext-guide-countdown-ring">{countdown}</span>
              <span className="ext-guide-countdown-label">second{countdown !== 1 ? 's' : ''} — continuing automatically</span>
            </div>
            <div className="ext-guide-confirm-btns">
              <button className="ext-guide-confirm-continue" onClick={() => setCountdown(0)}>
                ✓ Continue now
              </button>
              <button className="ext-guide-confirm-cancel" onClick={() => { setCountdown(3); setStep('choose'); }}>
                ✕ Cancel
              </button>
            </div>
          </div>
        )}

        {step === 'auto-running' && (
          <div className="ext-guide-auto-running">
            <div className="ext-guide-spinner" />
            <p><strong>Installing…</strong></p>
            <p className="ext-guide-auto-hint">This browser window will close shortly.</p>
            <p className="ext-guide-auto-hint">Edge will reopen automatically once installation is complete.</p>
          </div>
        )}

        {step === 'auto-done' && (
          <>
            <div className={`ext-guide-result ${autoOk ? 'success' : 'failure'}`}>
              {autoOk ? '✅ Extension installed! Reopening Edge…' : '❌ Installation failed'}
            </div>
            <pre className="ext-guide-output">{autoOutput}</pre>
            {autoOk && (
              <p className="ext-guide-note" style={{ marginTop: 8 }}>
                Edge is reopening to this page automatically.
              </p>
            )}
            {!autoOk && (
              <p className="ext-guide-note" style={{ marginTop: 8 }}>
                Try the <button className="ext-guide-link-btn" onClick={() => setStep('manual')}>manual steps</button> or re-run the installer from a terminal.
              </p>
            )}
            <div className="ext-guide-footer">
              <button className="ext-guide-done-btn" onClick={onClose}>Close</button>
            </div>
          </>
        )}

        {step === 'manual' && (
          <>
            <ol className="ext-guide-steps">
              <li>
                <strong>Download</strong> the extension package:
                <button
                  className="ext-guide-download-btn"
                  onClick={(e) => {
                    e.stopPropagation();
                    const a = document.createElement('a');
                    a.href = '/api/extension_zip';
                    a.download = 'ai_secretary_extension.zip';
                    document.body.appendChild(a);
                    a.click();
                    document.body.removeChild(a);
                  }}
                >
                  ⬇ Download ai_secretary_extension.zip
                </button>
              </li>
              <li>
                <strong>Extract</strong> the zip file to a permanent folder
                <span className="ext-guide-hint">(e.g. <code>C:\tools\ai_secretary_extension</code>)</span>
              </li>
              <li>
                <strong>Open</strong> {browser} extensions page:
                <button
                  className="ext-guide-copy-btn"
                  onClick={() => { navigator.clipboard.writeText(extensionsUrl); }}
                  title="Copy to clipboard"
                >
                  📋 {extensionsUrl}
                </button>
              </li>
              <li><strong>Enable</strong> <em>"Developer mode"</em> (toggle in the top-right corner)</li>
              <li><strong>Click</strong> <em>"Load unpacked"</em> and select the extracted <code>browser_extension</code> folder</li>
              <li><strong>Pin</strong> the extension — click the puzzle icon (🧩) in the toolbar, then the pin icon next to "AI Secretary"</li>
            </ol>
            <div className="ext-guide-footer">
              <span className="ext-guide-note">
                After installing, refresh this page — the button will turn green ✓
              </span>
              <button className="ext-guide-done-btn" onClick={onClose}>Done</button>
            </div>
          </>
        )}
      </div>
    </div>
  );
};


interface Props {
  title: string;
  subtitle: string;
  datasetPath?: string;
  userName?: string;
  aiProvider?: string;
  aiModel?: string;
  pipelineStatus: { color: string; label: string; pulse: boolean; stageLabel?: string; progress?: number };
  pipelineCanStart: boolean;
  pipelineIsSleeping?: boolean;
  pipelineControlBusy: boolean;
  onTogglePipeline: () => void;
  onRefresh: () => void;
  onSettings: () => void;
  loading?: boolean;
  extensionInstalled?: boolean;
  scheduleConfig: ScheduleConfig;
  onSaveSchedule: (updates: Partial<ScheduleConfig>) => Promise<void>;
}

export const CompactHeader: React.FC<Props> = ({
  title,
  subtitle,
  datasetPath,
  userName,
  aiProvider,
  aiModel,
  pipelineStatus,
  pipelineCanStart,
  pipelineIsSleeping: _pipelineIsSleeping,
  pipelineControlBusy,
  onTogglePipeline,
  onRefresh,
  onSettings,
  loading,
  extensionInstalled,
  scheduleConfig,
  onSaveSchedule,
}) => {
  const [showExtGuide, setShowExtGuide] = useState(false);
  const [showScheduler, setShowScheduler] = useState(false);
  const schedBtnRef = useRef<HTMLButtonElement>(null);

  return (
    <>
    <header className="v2-header">
      <div className="v2-header-left">
        <span className="v2-header-logo">🤖</span>
        <div className="v2-header-titles">
          <span className="v2-header-title">{title}</span>
          {(datasetPath || userName) && (
            <span className="v2-header-meta">
              {userName || datasetPath}
            </span>
          )}
        </div>
      </div>

      <div className="v2-header-center">
        <span className="v2-header-subtitle">{subtitle}</span>
        <span
          className={`v2-status-dot ${pipelineStatus.pulse ? 'pulse' : ''}`}
          style={{ backgroundColor: pipelineStatus.color }}
          title={`Pipeline: ${pipelineStatus.label}`}
        />
        <span className="v2-header-status-label" style={{ color: pipelineStatus.color }}>
          {pipelineStatus.label}
        </span>
        {aiProvider && aiModel && (
          <span
            className={`v2-header-ai-badge ${aiProvider === 'copilot' ? 'copilot' : 'azure'}`}
            title={`AI: ${aiProvider} / ${aiModel}`}
          >
            {aiProvider === 'copilot' ? '🐙' : '☁️'} {aiModel}
          </span>
        )}
        {pipelineStatus.pulse && typeof pipelineStatus.progress === 'number' && (
          <div className="v2-header-pipeline-progress">
            <div className="v2-header-progress-track">
              <div
                className="v2-header-progress-fill"
                style={{ width: `${pipelineStatus.progress}%` }}
              />
            </div>
            <span className="v2-header-progress-text">
              {pipelineStatus.progress}%{pipelineStatus.stageLabel ? ` • ${pipelineStatus.stageLabel}` : ''}
            </span>
          </div>
        )}
      </div>

      <div className="v2-header-right">
        <button
          className={`v2-header-pipeline-btn ${
            pipelineCanStart ? 'start' : 'stop'
          }`}
          onClick={onTogglePipeline}
          disabled={pipelineControlBusy}
          title={pipelineControlBusy ? 'Working…' : pipelineCanStart ? 'Start Pipeline' : 'Stop Pipeline'}
        >
          {pipelineControlBusy ? '⏳' : pipelineCanStart ? '▶' : '■'}
          <span className="v2-pipeline-btn-label">
            {pipelineControlBusy ? 'Working…' : pipelineCanStart ? 'Start' : 'Stop'}
          </span>
        </button>

        {/* Scheduler icon */}
        <button
          ref={schedBtnRef}
          className={`v2-header-icon-btn v2-sched-btn ${scheduleConfig.schedule_enabled ? 'active' : ''}`}
          onClick={() => setShowScheduler(!showScheduler)}
          title={scheduleConfig.schedule_enabled ? 'Schedule active — click to configure' : 'Configure pipeline schedule'}
        >
          {/* Clock icon */}
          <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
            <circle cx="12" cy="12" r="10" />
            <polyline points="12 6 12 12 16 14" />
          </svg>
          {scheduleConfig.schedule_enabled && <span className="v2-sched-dot" />}
        </button>
        {showScheduler && (
          <SchedulerPopover
            anchorRef={schedBtnRef}
            onClose={() => setShowScheduler(false)}
            onSave={onSaveSchedule}
            initial={scheduleConfig}
          />
        )}

        {/* Browser Extension install / status button */}
        <button
          className={`v2-header-icon-btn v2-ext-btn ${extensionInstalled ? 'installed' : ''}`}
          onClick={() => setShowExtGuide(true)}
          title={extensionInstalled ? 'Browser extension installed ✓ — click to download latest' : 'Install browser extension'}
        >
          <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
            <path d="M20 16V8a2 2 0 0 0-2-2h-3a2 2 0 0 1-2-2 2 2 0 0 0-4 0 2 2 0 0 1-2 2H4a2 2 0 0 0-2 2v5a2 2 0 0 0 2 2 2 2 0 0 1 0 4 2 2 0 0 0-2 2v1a2 2 0 0 0 2 2h16a2 2 0 0 0 2-2v-1a2 2 0 0 1 0-4 2 2 0 0 0 0-4z" />
          </svg>
          {extensionInstalled && <span className="v2-ext-check">✓</span>}
        </button>

        <button
          className="v2-header-icon-btn"
          onClick={onRefresh}
          disabled={loading}
          title="Refresh"
        >
          <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
            <polyline points="23 4 23 10 17 10" />
            <path d="M20.49 15a9 9 0 1 1-2.12-9.36L23 10" />
          </svg>
        </button>

        <button
          className="v2-header-icon-btn"
          onClick={onSettings}
          title="Settings"
        >
          {/* Gear icon */}
          <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
            <circle cx="12" cy="12" r="3" />
            <path d="M19.4 15a1.65 1.65 0 0 0 .33 1.82l.06.06a2 2 0 1 1-2.83 2.83l-.06-.06a1.65 1.65 0 0 0-1.82-.33 1.65 1.65 0 0 0-1 1.51V21a2 2 0 0 1-4 0v-.09A1.65 1.65 0 0 0 9 19.4a1.65 1.65 0 0 0-1.82.33l-.06.06a2 2 0 1 1-2.83-2.83l.06-.06A1.65 1.65 0 0 0 4.68 15a1.65 1.65 0 0 0-1.51-1H3a2 2 0 0 1 0-4h.09A1.65 1.65 0 0 0 4.6 9a1.65 1.65 0 0 0-.33-1.82l-.06-.06a2 2 0 1 1 2.83-2.83l.06.06A1.65 1.65 0 0 0 9 4.68a1.65 1.65 0 0 0 1-1.51V3a2 2 0 0 1 4 0v.09a1.65 1.65 0 0 0 1 1.51 1.65 1.65 0 0 0 1.82-.33l.06-.06a2 2 0 1 1 2.83 2.83l-.06.06A1.65 1.65 0 0 0 19.4 9a1.65 1.65 0 0 0 1.51 1H21a2 2 0 0 1 0 4h-.09a1.65 1.65 0 0 0-1.51 1z" />
          </svg>
        </button>
      </div>
    </header>
    {/* Mobile-only progress strip */}
    {pipelineStatus.pulse && typeof pipelineStatus.progress === 'number' && (
      <div className="v2-header-mobile-progress">
        <div className="v2-header-mobile-progress-fill" style={{ width: `${pipelineStatus.progress}%` }} />
        <span className="v2-header-mobile-progress-label">
          {pipelineStatus.progress}%{pipelineStatus.stageLabel ? ` — ${pipelineStatus.stageLabel}` : ''}
        </span>
      </div>
    )}
    {showExtGuide && <ExtInstallGuide onClose={() => setShowExtGuide(false)} />}
    </>
  );
};
