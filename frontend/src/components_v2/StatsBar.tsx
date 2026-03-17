/**
 * StatsBar — Compact single-row stats replacing the legacy Dashboard tiles.
 */
import React from 'react';
import { DashboardStats, DashboardStatus, UserRole } from '../types';
import './StatsBar.css';

interface Props {
  stats: DashboardStats;
  activeStatus: DashboardStatus;
  activeRole: UserRole | null;
  onStatusChange: (status: DashboardStatus) => void;
  onRoleChange: (role: UserRole) => void;
}

export const StatsBar: React.FC<Props> = ({
  stats,
  activeStatus,
  activeRole,
  onStatusChange,
  onRoleChange,
}) => {
  const isRole = (role: UserRole) => activeStatus === 'active' && activeRole === role;
  const isDone = activeStatus === 'done';
  const doneCount = stats.doneTabCompletedCount + stats.doneTabDismissedCount;

  return (
    <div className="v2-stats-bar">
      <button
        className={`v2-stat-pill ${isRole('assignee') ? 'active role-assignee' : ''}`}
        onClick={() => onRoleChange('assignee')}
        title="Filter: Assignee"
      >
        Assignee <span className="v2-stat-count">{stats.activeAssigneeCount}</span>
      </button>

      <span className="v2-stat-sep">·</span>

      <button
        className={`v2-stat-pill ${isRole('collaborator') ? 'active role-collaborator' : ''}`}
        onClick={() => onRoleChange('collaborator')}
        title="Filter: Collaborator"
      >
        Collaborator <span className="v2-stat-count">{stats.activeCollaboratorCount}</span>
      </button>

      <span className="v2-stat-sep">·</span>

      <button
        className={`v2-stat-pill ${isRole('observer') ? 'active role-observer' : ''}`}
        onClick={() => onRoleChange('observer')}
        title="Filter: Watching"
      >
        Watching <span className="v2-stat-count">{stats.activeObserverCount + stats.activeWatchingCount}</span>
      </button>

      <span className="v2-stat-sep">·</span>

      <button
        className={`v2-stat-pill ${isDone ? 'active role-done' : ''}`}
        onClick={() => onStatusChange('done')}
        title="Filter: Done"
      >
        Done <span className="v2-stat-count">{doneCount}</span>
      </button>
    </div>
  );
};
