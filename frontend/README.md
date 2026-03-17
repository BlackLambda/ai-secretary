# AI Secretary - React TypeScript Frontend

This is the React TypeScript frontend for the AI Secretary briefing application.

## Setup

1. Install dependencies:
```powershell
cd frontend
npm install
```

## Development

To run the development server:

```powershell
# Terminal 1: Start Flask backend
cd ..
python server_react.py

# OR with custom data path
python server_react.py --data path/to/your/briefing_data.json

# Terminal 2: Start React dev server
cd frontend
npm run dev
```

This will start the Vite dev server on `http://localhost:3000`. The dev server is configured to proxy API requests to the Flask backend running on `http://localhost:5000`.

### Data Path Configuration

The Flask backend will look for briefing data in these locations (priority order):

1. **Command-line argument**: `--data path/to/briefing_data.json`
2. **Config file**: `briefing_data_path` key in `pipeline_config.json`
3. **Default**: `incremental_data/output/briefing_data.json`

Example with custom data path:
```powershell
python server_react.py --data C:\my_data\briefing_data.json
```

## Production Build

To build for production:

```powershell
cd frontend
npm run build
```

This will create optimized production files in the `../static` directory. The Flask server will automatically serve these files.

## Architecture

### Components

- **App.tsx** - Main application component with state management
- **Dashboard.tsx** - Statistics dashboard showing task counts
- **SideNav.tsx** - Sidebar navigation with grouped tasks
- **TaskItem.tsx** - Individual task/action item with complete/dismiss/promote actions
- **PipelineStatus.tsx** - Real-time pipeline status indicator

### State Management

The app uses React hooks for state management:
- `useState` for component state
- `useEffect` for side effects (data fetching, polling)
- Smart refresh: automatically reloads data when pipeline finishes working

### API Integration

The `api.ts` module provides functions to:
- Fetch briefing data
- Save user operations (complete, dismiss, promote)
- Get pipeline status
- Check for updates

### Features

- ✅ Real-time pipeline status with smart refresh
- ✅ Complete/Dismiss/Promote task actions
- ✅ Grouped sidebar navigation with filtering
- ✅ Statistics dashboard
- ✅ Persistent user operations
- ✅ Responsive layout
- ✅ Smooth scrolling to tasks

## Technology Stack

- **React 18** - UI framework
- **TypeScript** - Type safety
- **Vite** - Build tool and dev server
- **CSS Modules** - Component styling
- **Flask** - Backend API server
