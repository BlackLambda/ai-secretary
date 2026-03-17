/**
 * SchedulerPopover — Small popover for configuring pipeline schedule.
 *
 * Shows: toggle on/off, day checkboxes (Mon–Sun), start/end hour pickers, interval.
 * Reads/writes via `/api/pipeline_config`.
 */
import React, { useState, useEffect, useRef } from 'react';
import './SchedulerPopover.css';

export interface ScheduleConfig {
  schedule_enabled: boolean;
  schedule_days: number[];       // 1=Mon … 7=Sun (ISO weekday)
  schedule_start_hour: number;   // 0–23
  schedule_start_minute: number; // 0–59
  schedule_end_hour: number;     // 0–23
  schedule_end_minute: number;   // 0–59
  fetch_interval_minutes: number;
}

interface Props {
  anchorRef: React.RefObject<HTMLElement | null>;
  onClose: () => void;
  onSave: (updates: Partial<ScheduleConfig>) => Promise<void>;
  initial: ScheduleConfig;
}

const DAY_LABELS: { id: number; short: string; full: string }[] = [
  { id: 1, short: 'Mon', full: 'Monday' },
  { id: 2, short: 'Tue', full: 'Tuesday' },
  { id: 3, short: 'Wed', full: 'Wednesday' },
  { id: 4, short: 'Thu', full: 'Thursday' },
  { id: 5, short: 'Fri', full: 'Friday' },
  { id: 6, short: 'Sat', full: 'Saturday' },
  { id: 7, short: 'Sun', full: 'Sunday' },
];

const MINUTE_OPTIONS = Array.from({ length: 60 }, (_, i) => i);
const HOUR_OPTIONS = Array.from({ length: 24 }, (_, i) => i);

const formatTime = (h: number, m: number) => `${String(h).padStart(2, '0')}:${String(m).padStart(2, '0')}`;

export const SchedulerPopover: React.FC<Props> = ({ anchorRef, onClose, onSave, initial }) => {
  const popRef = useRef<HTMLDivElement>(null);

  const [enabled, setEnabled] = useState(initial.schedule_enabled);
  const [days, setDays] = useState<number[]>(initial.schedule_days);
  const [startHour, setStartHour] = useState(initial.schedule_start_hour);
  const [startMinute, setStartMinute] = useState(initial.schedule_start_minute);
  const [endHour, setEndHour] = useState(initial.schedule_end_hour);
  const [endMinute, setEndMinute] = useState(initial.schedule_end_minute);
  const [interval, setInterval_] = useState(initial.fetch_interval_minutes);
  const [saving, setSaving] = useState(false);
  const [dirty, setDirty] = useState(false);

  // Sync with incoming initial when it changes
  useEffect(() => {
    setEnabled(initial.schedule_enabled);
    setDays(initial.schedule_days);
    setStartHour(initial.schedule_start_hour);
    setStartMinute(initial.schedule_start_minute);
    setEndHour(initial.schedule_end_hour);
    setEndMinute(initial.schedule_end_minute);
    setInterval_(initial.fetch_interval_minutes);
    setDirty(false);
  }, [initial]);

  // Close on outside click
  useEffect(() => {
    const handler = (e: MouseEvent) => {
      if (
        popRef.current &&
        !popRef.current.contains(e.target as Node) &&
        anchorRef.current &&
        !anchorRef.current.contains(e.target as Node)
      ) {
        onClose();
      }
    };
    document.addEventListener('mousedown', handler);
    return () => document.removeEventListener('mousedown', handler);
  }, [onClose, anchorRef]);

  const toggleDay = (id: number) => {
    setDays((prev) => {
      const next = prev.includes(id) ? prev.filter((d) => d !== id) : [...prev, id].sort();
      return next;
    });
    setDirty(true);
  };

  const handleSave = async () => {
    setSaving(true);
    try {
      await onSave({
        schedule_enabled: enabled,
        schedule_days: days,
        schedule_start_hour: startHour,
        schedule_start_minute: startMinute,
        schedule_end_hour: endHour,
        schedule_end_minute: endMinute,
        fetch_interval_minutes: interval,
      });
      setDirty(false);
    } catch (e) {
      console.error('Failed to save schedule', e);
    } finally {
      setSaving(false);
    }
  };

  const presetWorkdays = () => { setDays([1, 2, 3, 4, 5]); setDirty(true); };
  const presetEveryday = () => { setDays([1, 2, 3, 4, 5, 6, 7]); setDirty(true); };

  return (
    <div className="sched-popover" ref={popRef}>
      <div className="sched-popover-header">
        <span className="sched-popover-title">⏰ Pipeline Schedule</span>
        <button className="sched-popover-close" onClick={onClose} title="Close">✕</button>
      </div>

      {/* Enable/disable toggle */}
      <label className="sched-toggle-row">
        <span className="sched-toggle-label">Schedule enabled</span>
        <button
          className={`sched-toggle-switch ${enabled ? 'on' : ''}`}
          onClick={() => { setEnabled(!enabled); setDirty(true); }}
          role="switch"
          aria-checked={enabled}
        >
          <span className="sched-toggle-knob" />
        </button>
      </label>

      {/* Day picker */}
      <div className={`sched-section ${!enabled ? 'disabled' : ''}`}>
        <div className="sched-section-label">
          Active days
          <span className="sched-presets">
            <button onClick={presetWorkdays} disabled={!enabled}>Weekdays</button>
            <button onClick={presetEveryday} disabled={!enabled}>Every day</button>
          </span>
        </div>
        <div className="sched-day-chips">
          {DAY_LABELS.map((d) => (
            <button
              key={d.id}
              className={`sched-day-chip ${days.includes(d.id) ? 'active' : ''}`}
              onClick={() => toggleDay(d.id)}
              disabled={!enabled}
              title={d.full}
            >
              {d.short}
            </button>
          ))}
        </div>
      </div>

      {/* Hours */}
      <div className={`sched-section ${!enabled ? 'disabled' : ''}`}>
        <div className="sched-section-label">Active hours</div>
        <div className="sched-hours-row">
          <label className="sched-hour-pick">
            <span>From</span>
            <div className="sched-time-selects">
              <select
                value={startHour}
                onChange={(e) => { setStartHour(Number(e.target.value)); setDirty(true); }}
                disabled={!enabled}
              >
                {HOUR_OPTIONS.map((h) => (
                  <option key={h} value={h}>{String(h).padStart(2, '0')}</option>
                ))}
              </select>
              <span className="sched-time-colon">:</span>
              <select
                value={startMinute}
                onChange={(e) => { setStartMinute(Number(e.target.value)); setDirty(true); }}
                disabled={!enabled}
              >
                {MINUTE_OPTIONS.map((m) => (
                  <option key={m} value={m}>{String(m).padStart(2, '0')}</option>
                ))}
              </select>
            </div>
          </label>
          <span className="sched-hour-sep">→</span>
          <label className="sched-hour-pick">
            <span>To</span>
            <div className="sched-time-selects">
              <select
                value={endHour}
                onChange={(e) => { setEndHour(Number(e.target.value)); setDirty(true); }}
                disabled={!enabled}
              >
                {HOUR_OPTIONS.map((h) => (
                  <option key={h} value={h}>{String(h).padStart(2, '0')}</option>
                ))}
              </select>
              <span className="sched-time-colon">:</span>
              <select
                value={endMinute}
                onChange={(e) => { setEndMinute(Number(e.target.value)); setDirty(true); }}
                disabled={!enabled}
              >
                {MINUTE_OPTIONS.map((m) => (
                  <option key={m} value={m}>{String(m).padStart(2, '0')}</option>
                ))}
              </select>
            </div>
          </label>
        </div>
        {enabled && (endHour * 60 + endMinute) <= (startHour * 60 + startMinute) && (
          <div className="sched-warn">⚠ End time should be after start time</div>
        )}
      </div>

      {/* Interval */}
      <div className="sched-section">
        <div className="sched-section-label">Run cycle interval</div>
        <div className="sched-interval-row">
          <span>Every</span>
          <select
            value={interval}
            onChange={(e) => { setInterval_(Number(e.target.value)); setDirty(true); }}
          >
            {[15, 30, 45, 60, 90, 120, 180, 240].map((m) => (
              <option key={m} value={m}>
                {m < 60 ? `${m} min` : `${m / 60} hr${m > 60 ? 's' : ''}`}
              </option>
            ))}
          </select>
        </div>
      </div>

      {/* Summary */}
      {enabled && (
        <div className="sched-summary">
          {days.length === 0
            ? 'No days selected — pipeline will not run.'
            : `Runs ${DAY_LABELS.filter((d) => days.includes(d.id)).map((d) => d.short).join(', ')} ${formatTime(startHour, startMinute)}–${formatTime(endHour, endMinute)}, every ${interval < 60 ? `${interval} min` : `${interval / 60} hr${interval > 60 ? 's' : ''}`}.`}
        </div>
      )}
      {!enabled && (
        <div className="sched-summary muted">Pipeline runs continuously (24/7).</div>
      )}

      {/* Save */}
      <div className="sched-popover-footer">
        <button
          className="sched-save-btn"
          onClick={handleSave}
          disabled={saving || !dirty}
        >
          {saving ? 'Saving…' : 'Save'}
        </button>
      </div>
    </div>
  );
};
