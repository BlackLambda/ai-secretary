# AI Secretary

AI Secretary is a local-first Outlook and Teams triage app. It fetches recent work activity, runs AI extraction over emails and chats, deduplicates and links related items, and serves the result through a Flask backend, a React dashboard, and a browser extension popup.

## Quick Start

The primary setup and launch path is:

```bash
./start.bat
```

`start.bat` is the canonical Windows launcher. It currently handles:

- protocol-handler registration for the browser extension
- update/restart protocol registration
- Python 3.11 prerequisite checks and installation when needed
- project-local `.venv` creation and reuse
- dependency installation into `.venv`
- server startup and browser launch

If you want to activate the same virtual environment manually, use:

```powershell
.\activate_venv.ps1
```

or from `cmd.exe`:

```cmd
activate_venv.bat
```

After startup, open:

- `http://localhost:5000`

## What The Project Does

The system is built to turn noisy Outlook and Teams activity into a daily action-oriented briefing.

At a high level it:

1. loads user profile and followed-topic context
2. incrementally fetches Outlook email and Teams data
3. processes raw messages into threads and conversations
4. runs AI extraction and validation stages
5. merges Outlook and Teams results into a unified briefing dataset
6. serves the briefing in the web dashboard and extension popup

## Current Architecture

### Backend

The active server entry point is `server_react.py`.

It is responsible for:

- serving the built frontend from `static/app/`
- exposing briefing and pipeline APIs
- managing pipeline state and settings
- handling Azure and Copilot auth/model APIs
- supporting the browser extension install/download flow
- exposing update-check and app-control endpoints

Important API areas include:

- `/api/briefing_data`
- `/api/pipeline_status`
- `/api/pipeline_config`
- `/api/check_update`
- `/api/azure/*`
- `/api/copilot/*`
- `/api/extension_zip`
- `/api/install_extension`

### Frontend

The current UI is the React v2 app in `frontend/src/components_v2/`.

Main pieces:

- `AppShell` as the top-level application shell
- `CompactHeader` for pipeline controls and app actions
- `OnboardingWizard` for first-run setup
- `SettingsDrawer` for AI, pipeline, topics, and reset/config operations
- `StatsBar` and `CardStream` for briefing exploration

The frontend supports:

- first-run onboarding
- pipeline start/stop controls
- scheduling support
- update banners
- AI backend/model selection
- browser extension detection
- persistent user actions and filtering

### Browser Extension

The `browser_extension/` directory contains the Chrome/Edge extension.

The extension:

- reads briefing data from the local API
- shows pipeline status and prioritized items in a compact popup
- opens the full dashboard for deeper interaction
- can launch the local server through the custom protocol flow

### Pipeline

The main orchestrator is `pipeline/run_incremental_pipeline.py`.

The current pipeline includes:

- incremental fetch and merge stages
- step planning/status reporting for the UI
- pruning of old data
- tee logging into `user_state/pipeline.log`
- schedule-aware execution
- configurable Outlook worker concurrency
- parallel Outlook extraction above a threshold
- per-worker colored logs

Current Outlook worker settings:

- config key: `outlook_parallel_workers`
- min: `1`
- max: `5`
- default: `3`

## AI Backends

The app currently supports two AI backends:

- GitHub Copilot
- Azure OpenAI

The current UI defaults to Copilot with `gemini-3-flash-preview`, but backend and model remain configurable from settings/onboarding.

## Data Flow

The effective runtime flow is:

```text
User profile / topics
	-> incremental fetch
	-> Outlook thread processing
	-> Teams conversation processing
	-> merge into master data
	-> prune old data
	-> Outlook AI extraction / validation / dedup
	-> Teams AI analysis / dedup
	-> unified briefing preparation
	-> dashboard + extension consumption
```

### Outlook side

Primary Outlook pipeline modules live in `outlook_v2/`:

- `process_threads.py`
- `ai_extract_events.py`
- `ai_extract_actions.py`
- `ai_validate_actions.py`
- `ai_dedup_events.py`
- `ai_dedup_todos.py`
- `ai_link_teams_to_outlook.py`

### Teams side

Primary Teams pipeline modules live in `teams/`:

- `process_teams_messages.py`
- `analyze_teams_conversations.py`
- `dedup_todos.py`

## Configuration

The project uses layered config:

- defaults in `config/pipeline_config.default.json`
- local effective config in `config/pipeline_config.json`
- merge/save logic in `lib/pipeline_config_manager.py`

The settings UI currently exposes operational values such as:

- AI backend and model
- schedule window
- fetch interval
- pruning threshold
- Outlook worker count

Some setup values are intentionally first-run only and are configured through onboarding rather than the main settings surface.

## Common Local Workflows

### Recommended: desktop-style launch

```bash
./start.bat
```

### Run backend directly

```bash
python server_react.py
```

### Frontend development

```bash
cd frontend
npm install
npm run dev
```

### Production frontend build

```bash
cd frontend
npm run build
```

## Bug Reports

When a bug report is submitted from the UI, the backend writes a JSON payload under `incremental_data/bug_reports/` and uses a best-effort mailto/draft flow so the user can attach and send it manually from Outlook Web.

Relevant endpoint:

- `GET /api/bug_reports/<filename>`

## Main Directories

```text
ai_secretary_core/   Core helpers for paths, state, focus, and JSON utilities
browser_extension/   Browser extension popup and settings
config/              Default and effective pipeline config
docs/                Non-canonical supporting assets and notes
frontend/            React + TypeScript frontend
incremental_data/    Generated runtime data and outputs
lib/                 Shared backend utilities, AI clients, config handling
outlook_v2/          Outlook extraction / validation / dedup pipeline
pipeline/            Pipeline orchestration and support scripts
teams/               Teams processing and AI analysis
user_state/          Local runtime state, logs, and user operations
```

## Documentation Policy

This `README.md` is the canonical project document.

If other docs drift from the codebase, prefer updating or deleting them rather than creating competing summaries.
