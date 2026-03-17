# AI Secretary — Browser Extension

A Chrome/Edge extension that shows your AI Secretary prioritized tasks in a popup, right from the browser toolbar.

## Features

- **Task overview** — see all active tasks sorted by priority score
- **Filter tabs** — filter by All / High priority / Outlook / Teams
- **Pipeline status** — live indicator (working / sleeping / offline)
- **Badge count** — active task count shown on the extension icon
- **Auto-refresh** — background polling keeps the badge updated
- **Configurable** — set server URL and refresh interval in Settings

## Installation

### 1. Start the AI Secretary server

```bash
python server_react.py
```

The server must be running on `http://localhost:5000` (default).

### 2. Load the extension in Chrome / Edge

1. Open `chrome://extensions` (or `edge://extensions`)
2. Enable **Developer mode** (toggle in top-right)
3. Click **Load unpacked**
4. Select the `browser_extension/` folder from this repo
5. The AI Secretary icon appears in your toolbar

### 3. (Optional) Pin the extension

Click the puzzle-piece icon in the toolbar → find "AI Secretary" → click the pin icon.

## Configuration

Click **⚙ Settings** in the popup footer (or right-click the icon → Options):

| Setting | Default | Description |
|---|---|---|
| Server URL | `http://localhost:5000` | URL of your AI Secretary server |
| Refresh interval | 30 seconds | How often the background badge updates |

## How It Works

- **Popup** (`popup.html/js/css`) — fetches `/api/briefing_data` and `/api/pipeline_status`, renders cards grouped by event with action items
- **Background** (`background.js`) — periodically fetches briefing data and updates the toolbar badge with active task count
- **Options** (`options.html`) — settings page for server URL and refresh interval
- **CORS** — the Flask server allows `chrome-extension://` and `moz-extension://` origins via an `@app.after_request` handler

## File Structure

```
browser_extension/
├── manifest.json       # Extension manifest (MV3)
├── popup.html          # Popup UI
├── popup.css           # Popup styles (dark theme)
├── popup.js            # Popup logic
├── background.js       # Service worker (badge updates)
├── options.html        # Settings page
├── icons/
│   ├── icon16.png
│   ├── icon48.png
│   └── icon128.png
└── README.md           # This file
```
