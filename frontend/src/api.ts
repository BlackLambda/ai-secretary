import { BriefingData, UserOperations, PipelineStatus } from './types';

const API_BASE = '/api';

export const api = {
  // Fetch briefing data
  async getBriefingData(): Promise<BriefingData> {
    const response = await fetch(`${API_BASE}/briefing_data`);
    const data: any = await response.json().catch(() => null);
    if (!response.ok) {
      const message = data?.error || data?.message || 'Failed to fetch briefing data';
      const err: any = new Error(message);
      err.status = response.status;
      err.payload = data;
      throw err;
    }
    return data;
  },

  // Save user operation (complete, dismiss)
  async saveOperation(
    type: 'complete' | 'dismiss' | 'disable_event_type' | 'pin_card',
    id: string,
    active: boolean,
    context?: any,
  ): Promise<UserOperations> {
    const response = await fetch(`${API_BASE}/save_operation`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({ type, id, active, context }),
    });
    
    if (!response.ok) {
      throw new Error('Failed to save operation');
    }
    
    const data = await response.json();
    return data.ops;
  },

  /** @deprecated Legacy per-card like/dislike API kept for compatibility only. */
  async sendCardFeedback(
    card_type: 'Outlook' | 'Teams',
    card_key: string,
    feedback: 'like' | 'dislike' | 'none',
  ): Promise<{ status: string; card_feedback: Record<string, 'like' | 'dislike'>; focus_model?: any }> {
    const response = await fetch(`${API_BASE}/card_feedback`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({ card_type, card_key, feedback }),
    });

    const data: any = await response.json().catch(() => null);
    if (!response.ok) {
      const msg = data?.error || 'Failed to save card feedback';
      throw new Error(msg);
    }
    return data;
  },

  // Deprecated back-compat endpoint: like => move into following, dislike => move into not_following
  async updateWatchItem(value: string, action: 'like' | 'dislike'): Promise<{ status: string; following: string[] }> {
    const response = await fetch(`${API_BASE}/update_watch_item`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({ value, action }),
    });

    if (!response.ok) {
      throw new Error('Failed to update watch item');
    }

    return response.json();
  },

  async getTopicsState(): Promise<{ following: string[]; not_following: string[]; topics?: any; user_topics?: any; profile?: any }> {
    const response = await fetch(`${API_BASE}/topics_state`);
    const data: any = await response.json().catch(() => null);
    if (!response.ok) {
      const msg = data?.error || 'Failed to fetch topics state';
      throw new Error(msg);
    }
    return data;
  },

  async setTopicTarget(topic: string, target: 'following' | 'not_following'): Promise<{ following: string[]; not_following: string[]; topics?: any; user_topics?: any; profile?: any }> {
    const response = await fetch(`${API_BASE}/topics_state`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({ topic, target }),
    });

    const data: any = await response.json().catch(() => null);
    if (!response.ok) {
      const msg = data?.error || 'Failed to update topic';
      throw new Error(msg);
    }
    return data;
  },

  /** Replace all focus-derived topics (clears old ones, sets new). */
  async replaceFocusTopics(topics: string[]): Promise<{ following: string[]; not_following: string[]; topics?: any; user_topics?: any; profile?: any }> {
    const response = await fetch(`${API_BASE}/topics_state/replace_focus`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ topics }),
    });
    const data: any = await response.json().catch(() => null);
    if (!response.ok) {
      const msg = data?.error || 'Failed to replace focus topics';
      throw new Error(msg);
    }
    return data;
  },

  async getUserProfile(): Promise<{ profile: any }> {
    const response = await fetch(`${API_BASE}/user_profile`);
    const data: any = await response.json().catch(() => null);
    if (!response.ok) {
      const msg = data?.error || 'Failed to fetch user profile';
      throw new Error(msg);
    }
    return data;
  },

  async saveUserProfile(profile: any): Promise<{ status: string; profile: any; following?: string[] }> {
    const response = await fetch(`${API_BASE}/user_profile`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({ profile }),
    });

    const data: any = await response.json().catch(() => null);
    if (!response.ok) {
      const msg = data?.error || 'Failed to save user profile';
      throw new Error(msg);
    }
    return data;
  },

  async refetchUserProfile(): Promise<{ status: string; profile: any; fetch?: any; merge?: any; ext_profile_present?: boolean; ext_profile_cleared_keys?: string[] }> {
    const response = await fetch(`${API_BASE}/refetch_user_profile`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({}),
    });

    const data: any = await response.json().catch(() => null);
    if (!response.ok) {
      const msg = data?.error || 'Failed to refetch user profile';
      throw new Error(msg);
    }
    return data;
  },

  async analyzeRecentFocus(days: number = 7): Promise<any> {
    const response = await fetch(`${API_BASE}/analyze_recent_focus`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({ days }),
    });

    const data: any = await response.json().catch(() => null);
    if (!response.ok) {
      const msg = data?.error || 'Failed to analyze recent focus';
      throw new Error(msg);
    }
    return data;
  },

  async getFocusAnalysisProgress(): Promise<{
    status: string;
    step: number;
    total: number;
    label: string;
    percent: number;
  }> {
    const response = await fetch(`${API_BASE}/focus_analysis_progress`);
    const data: any = await response.json().catch(() => ({
      status: 'error',
      step: 0,
      total: 0,
      label: '',
      percent: 0,
    }));
    return data;
  },

  async getRecentFocus(): Promise<any> {
    const response = await fetch(`${API_BASE}/recent_focus`);
    const data: any = await response.json().catch(() => null);
    if (!response.ok) {
      const msg = data?.error || 'Failed to load recent focus';
      throw new Error(msg);
    }
    return data;
  },

  async deleteRecentFocusTopic(index: number, name?: string): Promise<any> {
    const response = await fetch(`${API_BASE}/recent_focus/delete`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ index, name }),
    });
    const data: any = await response.json().catch(() => null);
    if (!response.ok) throw new Error(data?.error || 'Failed to delete recent focus topic');
    return data;
  },

  async clearRecentFocus(): Promise<void> {
    const response = await fetch(`${API_BASE}/recent_focus/clear`, { method: 'POST' });
    const data: any = await response.json().catch(() => null);
    if (!response.ok) throw new Error(data?.error || 'Failed to clear recent focus');
  },

  async cancelRecentFocus(): Promise<void> {
    await fetch(`${API_BASE}/analyze_recent_focus/cancel`, { method: 'POST' }).catch(() => null);
  },

  // Get pipeline status
  async getPipelineStatus(): Promise<PipelineStatus> {
    const response = await fetch(`${API_BASE}/pipeline_status`);
    if (!response.ok) {
      throw new Error('Failed to fetch pipeline status');
    }
    return response.json();
  },

  async startPipeline(): Promise<{ status: string; pid?: number; stdout?: string; stderr?: string; error?: string }> {
    const response = await fetch(`${API_BASE}/pipeline_start`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({}),
    });

    const data = await response.json().catch(() => ({}));
    if (!response.ok) {
      const msg = (data as any)?.error || 'Failed to start pipeline';
      throw new Error(msg);
    }
    return data;
  },

  async stopPipeline(): Promise<{ status: string; pid?: number; exit_code?: number | null; error?: string }> {
    const response = await fetch(`${API_BASE}/pipeline_stop`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({}),
    });

    const data = await response.json().catch(() => ({}));
    if (!response.ok) {
      const msg = (data as any)?.error || 'Failed to stop pipeline';
      throw new Error(msg);
    }
    return data;
  },

  async resetPipeline(): Promise<{ status: string; pid?: number; deleted_incremental_data?: boolean; error?: string }> {
    const response = await fetch(`${API_BASE}/pipeline_reset`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({}),
    });

    const data = await response.json().catch(() => ({}));
    if (!response.ok) {
      const msg = (data as any)?.error || 'Failed to reset pipeline';
      throw new Error(msg);
    }
    return data;
  },

  async rerunExtract(
    mode: 'clean' | 'keep' | 'events',
    sources?: 'outlook' | 'teams' | 'both'
  ): Promise<{ status: string; pid?: number; mode: string; cleanup?: any; error?: string }> {
    const response = await fetch(`${API_BASE}/rerun_extract`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({ mode, sources }),
    });

    const data = await response.json().catch(() => ({}));
    if (!response.ok) {
      const msg = (data as any)?.error || 'Failed to rerun extract';
      throw new Error(msg);
    }
    return data;
  },

  async saveOutputExport(
    filename: string,
    data: any,
    opts?: { copyToOneDrive?: boolean; oneDriveDir?: string }
  ): Promise<{ status: string; filepath: string; onedrive_filepath?: string | null; onedrive_error?: string | null }> {
    const response = await fetch(`${API_BASE}/output_export`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({
        filename,
        data,
        copy_to_onedrive: !!opts?.copyToOneDrive,
        onedrive_dir: String(opts?.oneDriveDir || '').trim() || undefined,
      }),
    });

    const payload: any = await response.json().catch(() => ({}));
    if (!response.ok) {
      const msg = payload?.error || 'Failed to save output export';
      throw new Error(msg);
    }
    return payload;
  },

  async buildTopTasksAdaptiveCard(
    briefing_data: any,
    opts?: { limit?: number; task_todo_score_cutoff?: number; list_key?: string }
  ): Promise<any> {
    const response = await fetch(`${API_BASE}/top_tasks_adaptive_card`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({
        briefing_data,
        limit: opts?.limit,
        task_todo_score_cutoff: opts?.task_todo_score_cutoff,
        list_key: opts?.list_key,
      }),
    });

    const payload: any = await response.json().catch(() => ({}));
    if (!response.ok) {
      const msg = payload?.error || 'Failed to generate adaptive card export';
      throw new Error(msg);
    }
    return payload?.card;
  },

  async getTodosMonitorStatus(): Promise<any> {
    const response = await fetch(`${API_BASE}/todos_monitor/status`);
    const data: any = await response.json().catch(() => null);
    if (!response.ok) {
      const msg = data?.error || 'Failed to fetch todos monitor status';
      throw new Error(msg);
    }
    return data;
  },

  async setTodosMonitorConfig(opts: { enabled?: boolean; auto_start?: boolean; interval_sec?: number }): Promise<any> {
    const response = await fetch(`${API_BASE}/todos_monitor/config`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify(opts || {}),
    });

    const data: any = await response.json().catch(() => null);
    if (!response.ok) {
      const msg = data?.error || 'Failed to update todos monitor config';
      throw new Error(msg);
    }
    return data;
  },

  async runTodosMonitorOnce(): Promise<any> {
    const response = await fetch(`${API_BASE}/todos_monitor/run_once`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({}),
    });

    const data: any = await response.json().catch(() => null);
    if (!response.ok) {
      const msg = data?.error || 'Failed to run todos job';
      throw new Error(msg);
    }
    return data;
  },

  async getAppConfig(): Promise<{
    bug_report_recipient_email: string;
    app_update_poll_interval_seconds?: number;
    active_data_folder?: string;
    output_dir?: string;
    output_export_dir?: string;
    onedrive_adaptive_card_path?: string;
    onedrive_adaptive_card_dir?: string;
    onedrive_adaptive_card_available?: boolean;
    onedrive_adaptive_card_error?: string;
    user_display_name?: string;
  }> {
    const response = await fetch(`${API_BASE}/app_config`);
    if (!response.ok) {
      throw new Error('Failed to fetch app config');
    }
    return response.json();
  },

  async getPipelineConfig(): Promise<{ config: any }> {
    const response = await fetch(`${API_BASE}/pipeline_config`);
    const data: any = await response.json().catch(() => null);
    if (!response.ok) {
      const msg = data?.error || 'Failed to fetch pipeline config';
      throw new Error(msg);
    }
    return data;
  },

  async savePipelineConfig(updates: Record<string, any>): Promise<{ status: string; config: any }> {
    const response = await fetch(`${API_BASE}/pipeline_config`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({ updates }),
    });

    const data: any = await response.json().catch(() => null);
    if (!response.ok) {
      const msg = data?.error || 'Failed to save pipeline config';
      throw new Error(msg);
    }
    return data;
  },

  async getScoringRubrics(): Promise<{ outlook: any; teams: any }> {
    const response = await fetch(`${API_BASE}/scoring_rubrics`);
    if (!response.ok) {
      throw new Error('Failed to fetch scoring rubrics');
    }
    return response.json();
  },

  async getScoringSystemOutlook(): Promise<{ active: any }> {
    const response = await fetch(`${API_BASE}/scoring_system_outlook`);
    const data: any = await response.json().catch(() => null);
    if (!response.ok) {
      const msg = data?.error || 'Failed to fetch scoring system';
      throw new Error(msg);
    }
    return data;
  },

  async saveScoringSystemOutlook(rubric: Record<string, any>): Promise<{ status: string; active: any }> {
    const response = await fetch(`${API_BASE}/scoring_system_outlook`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({ rubric }),
    });

    const data: any = await response.json().catch(() => null);
    if (!response.ok) {
      const msg = data?.error || 'Failed to save scoring system';
      throw new Error(msg);
    }
    return data;
  },

  async resetScoringSystemOutlookToDefault(): Promise<{ status: string; active: any }> {
    const response = await fetch(`${API_BASE}/scoring_system_outlook/reset_default`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({}),
    });

    const data: any = await response.json().catch(() => null);
    if (!response.ok) {
      const msg = data?.error || 'Failed to reset scoring system';
      throw new Error(msg);
    }
    return data;
  },

  async recomputePriorities(): Promise<{
    status: string;
    data_path?: string;
    items_seen: number;
    items_updated: number;
    items_missing_breakdown: number;
    changes?: Array<{
      ui_id?: string | null;
      title?: string | null;
      source?: string;
      before?: { priority?: any; priority_score?: any; priority_score_max?: any };
      after?: { priority?: any; priority_score?: any; priority_score_max?: any };
    }>;
  }> {
    const response = await fetch(`${API_BASE}/recompute_priorities`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({}),
    });

    const data: any = await response.json().catch(() => null);
    if (!response.ok) {
      const msg = data?.error || 'Failed to recompute priorities';
      throw new Error(msg);
    }
    return data;
  },

  async getUserOpsRestoreStatus(): Promise<{
    can_restore: boolean;
    can_backup?: boolean;
    store_exists?: boolean;
    store_count?: number;
    user_op_exists?: boolean;
    user_op_has_any?: boolean;
  }> {
    const response = await fetch(`${API_BASE}/user_ops_restore_status`);
    if (!response.ok) {
      throw new Error('Failed to get user ops restore status');
    }
    return response.json();
  },

  async backupUserOperationToStore(): Promise<{ status: string; store_exists?: boolean; store_count?: number; stdout?: string; stderr?: string }> {
    const response = await fetch(`${API_BASE}/backup_user_operation_to_store`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({}),
    });

    const data = await response.json().catch(() => ({}));
    if (!response.ok) {
      const msg = (data as any)?.error || 'Failed to backup user operation';
      throw new Error(msg);
    }
    return data;
  },

  async restoreUserOpsViaAI(): Promise<{ status: string; ops?: UserOperations; stdout?: string }> {
    const response = await fetch(`${API_BASE}/restore_user_ops_via_ai`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({}),
    });

    if (!response.ok) {
      const text = await response.text();
      throw new Error(text || 'Failed to restore user ops');
    }
    return response.json();
  },

  // Get version (for auto-refresh)
  async getVersion(): Promise<string> {
    const response = await fetch(`${API_BASE}/version`);
    if (!response.ok) {
      throw new Error('Failed to fetch version');
    }
    const data = await response.json();
    return data.version;
  },

  // Report bug
  async reportBug(bugData: any): Promise<{ status: string; filename: string; filepath?: string }> {
    const response = await fetch(`${API_BASE}/report_bug`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify(bugData),
    });
    
    if (!response.ok) {
      throw new Error('Failed to report bug');
    }
    
    return response.json();
  },

  // Mock email the saved bug report JSON (server-side file)
  async emailBugReport(params: { filepath: string; to: string; subject?: string; body?: string }): Promise<{ status: string; mock_email_file: string }> {
    const response = await fetch(`${API_BASE}/email_bug_report`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify(params),
    });

    if (!response.ok) {
      throw new Error('Failed to email bug report');
    }

    return response.json();
  },

  // Windows-only helper: copy bug report JSON file reference to OS clipboard
  async copyBugReportToClipboard(params: { filename?: string; filepath?: string }): Promise<{ status: string; filepath: string }> {
    const response = await fetch(`${API_BASE}/copy_bug_report_to_clipboard`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify(params),
    });

    if (!response.ok) {
      throw new Error('Failed to copy bug report to clipboard');
    }

    return response.json();
  },

  // Read-only: check whether the repo is behind its upstream.
  async getAppUpdateStatus(): Promise<{
    update_available: boolean;
    behind_by: number;
    checking?: boolean;
    last_checked?: string | null;
    error?: string | null;
    branch?: string | null;
    upstream?: string | null;
  }> {
    const response = await fetch(`${API_BASE}/app_update_status`);
    if (!response.ok) {
      throw new Error('Failed to fetch app update status');
    }
    return response.json();
  },

  // Return available model lists per AI provider.
  async getAiModels(): Promise<{ azure: string[]; copilot: string[] }> {
    const response = await fetch(`${API_BASE}/ai/models`);
    return response.json();
  },

  // Ping the AI backend with a tiny test prompt.
  async aiPing(provider?: string, model?: string): Promise<{ ok: boolean; reply?: string; model?: string; elapsed_s?: number; error?: string }> {
    const response = await fetch(`${API_BASE}/ai/ping`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ provider: provider || '', model: model || '' }),
    });
    return response.json();
  },

  // Trigger `az login` on the backend — opens the default browser for Azure auth.
  async triggerAzureLogin(): Promise<{ logged_in: boolean; account?: any; error?: string }> {
    const response = await fetch(`${API_BASE}/azure/login`, { method: 'POST' });
    const data: any = await response.json().catch(() => null);
    return data ?? { logged_in: false, error: 'No response from server' };
  },

  // Read-only: check whether Azure CLI is logged in on the backend machine.
  async getAzureStatus(): Promise<{ logged_in: boolean; account?: any; error?: string }> {
    const response = await fetch(`${API_BASE}/azure/status`);
    const data: any = await response.json().catch(() => null);
    if (!response.ok) {
      const msg = data?.error || 'Failed to fetch Azure login status';
      throw new Error(msg);
    }
    return data;
  },

  // Read-only: check whether GitHub Copilot credentials are available.
  async getCopilotStatus(): Promise<{ logged_in: boolean; error?: string }> {
    const response = await fetch(`${API_BASE}/copilot/status`);
    const data: any = await response.json().catch(() => null);
    if (!response.ok) {
      const msg = data?.error || 'Failed to fetch Copilot status';
      throw new Error(msg);
    }
    return data;
  },

  // Start GitHub device-flow login for Copilot.
  async copilotLogin(): Promise<{
    user_code: string;
    verification_uri: string;
    device_code: string;
    interval: number;
    expires_in: number;
  }> {
    const response = await fetch(`${API_BASE}/copilot/login`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({}),
    });
    const data: any = await response.json().catch(() => null);
    if (!response.ok) {
      const msg = data?.error || 'Failed to start Copilot login';
      throw new Error(msg);
    }
    return data;
  },

  // Poll GitHub device-flow for completion (call this periodically after copilotLogin).
  async copilotLoginPoll(device_code: string): Promise<{
    status: 'pending' | 'complete' | 'slow_down' | 'error';
    logged_in?: boolean;
    error?: string;
  }> {
    const response = await fetch(`${API_BASE}/copilot/login_poll`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ device_code }),
    });
    const data: any = await response.json().catch(() => null);
    if (!response.ok) {
      return { status: 'error', error: data?.error || 'Poll failed' };
    }
    return data;
  },

  // Long-poll: server blocks until device flow completes or times out (up to 5 min).
  async copilotLoginWait(device_code: string, interval?: number, expires_in?: number, signal?: AbortSignal): Promise<{
    status: 'complete' | 'error';
    logged_in?: boolean;
    error?: string;
  }> {
    const response = await fetch(`${API_BASE}/copilot/login_wait`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ device_code, interval: interval || 5, expires_in: expires_in || 300 }),
      signal,
    });
    const data: any = await response.json().catch(() => null);
    if (!response.ok) {
      return { status: 'error', error: data?.error || 'Login failed' };
    }
    return data;
  },

  // Read-only: preview a repo file or directory listing.
  async getFilePreview(
    path: string,
    opts?: { maxBytes?: number },
  ): Promise<
    | { kind: 'file'; path: string; size?: number | null; truncated?: boolean; is_text?: boolean; content: string }
    | { kind: 'dir'; path: string; entries: Array<{ name: string; path: string; kind: 'file' | 'dir'; size?: number | null }> }
  > {
    const qs: string[] = [`path=${encodeURIComponent(path)}`];
    if (opts?.maxBytes && Number.isFinite(opts.maxBytes) && (opts.maxBytes as number) > 0) {
      qs.push(`max_bytes=${encodeURIComponent(String(opts.maxBytes))}`);
    }
    const url = `${API_BASE}/file_preview?${qs.join('&')}`;
    const response = await fetch(url);
    const data: any = await response.json().catch(() => null);
    if (!response.ok) {
      const msg = data?.error || 'Failed to preview file';
      throw new Error(msg);
    }
    return data;
  },

  /** Lightweight heartbeat to keep the server alive. */
  async heartbeat(): Promise<void> {
    try {
      await fetch(`${API_BASE}/heartbeat`, { method: 'POST' });
    } catch {
      // Silently ignore — server may be down.
    }
  },

  /** Check if a newer version is available on origin/main. */
  async checkUpdate(): Promise<{ has_update: boolean; server_stale?: boolean; current?: string; latest?: string; server_commit?: string; message?: string }> {
    const resp = await fetch(`${API_BASE}/check_update`);
    if (!resp.ok) return { has_update: false };
    return resp.json();
  },

  async getAiErrors(since?: string): Promise<Array<{ timestamp: string; message: string }>> {
    const url = since ? `${API_BASE}/ai_errors?since=${encodeURIComponent(since)}` : `${API_BASE}/ai_errors`;
    const resp = await fetch(url);
    if (!resp.ok) return [];
    const data = await resp.json();
    return data.errors ?? [];
  },

  /** Notify server the browser tab is closing. Uses sendBeacon for reliability. */
  sendClosingSignal(): void {
    try {
      const url = `${API_BASE}/client_closing`;
      if (navigator.sendBeacon) {
        navigator.sendBeacon(url);
      } else {
        fetch(url, { method: 'POST', keepalive: true }).catch(() => {});
      }
    } catch {
      // Best effort.
    }
  },
};
