# SubstrateDataExtract

Automated Microsoft 365 data extraction: emails, Teams messages, calendar events, and meeting transcripts. Set up once, runs daily at 8:00 AM Beijing time.

---

## 📌 Quick Reference (Updated 11.13)

### Daily Scheduler Commands

```powershell
# Start scheduler (runs 24/7 in background)
.\start_scheduler.bat

# Check if running and view logs
.\check_scheduler.bat

# Stop scheduler
.\stop_scheduler.bat

# View real-time logs
Get-Content daily_scheduler.log -Wait -Tail 50
```
---

---

## 📚 General Setup Guide

*The sections below provide complete setup instructions for new users.*

---

## What You'll Need

Before starting, make sure you have:
- ✅ **Microsoft account** with access to Outlook/Teams (work or personal)
- ✅ **Windows 10 or Windows 11** devbox
- ✅ **Internet connection** for downloading tools and accessing Microsoft APIs
- ✅ **~15 minutes** for initial setup

**No coding experience required!** Just follow the steps below.

---

## Quick Start Guide

**Open PowerShell and run:**

### Step 1: Install Prerequisites

#### 1.1 Install Python

**Run PowerShell as Administrator and execute:**

```powershell
.\install_python.ps1
```

This script will:
- Detect your system architecture (AMD64/ARM64/x86)
- Download and install Python 3.11.9
- Configure PATH automatically

#### 1.2 Install Git

Download and install from: https://git-scm.com/download/win

Use default settings during installation.

### Step 2: Open the Vendored Folder

```powershell
# Navigate to the vendored folder inside the main repo
cd <path-to-ai-secretary>\SubstrateDataExtraction

```

### Step 3: Install Python Dependencies

```powershell
pip install requests msal PyJWT playwright
playwright install
```

### Step 4: Daily Automation Setup

**Simple 3-step process to set up automated daily data extraction:**

#### Step 4.1: Initial Setup (One-time, 5 minutes)

```bash
# Run this once to identify your top collaborators
python m365_extractor.py --setup
```

**What happens:**
1. Extracts your top 20 collaborators
2. Opens file for review: `output/daily/collaborators.json`
3. **IMPORTANT**: Edit the file to add/remove people as needed
4. Press ENTER when done reviewing
5. Verification complete - ready for daily extraction!

**Result**: `output/daily/collaborators.json` (your verified collaborator list)

---

#### Step 4.2: Historical Backfill (Optional, One-time, 10 minutes)

**Extract past 14 days of data:**

```bash
# Backfill 14 days (default)
python m365_extractor.py --backfill

# Or specify number of days
python m365_extractor.py --backfill --days 30
```

**What happens:**
- Extracts daily data for each day in the range
- Uses same 3-step workflow automatically
- Saves to date-prefixed files (e.g., `2025-11-06_email_exchanges.json`)
- Shows progress: "Day 1/14: 2025-10-24"

**Result**: Historical data files for past N days

---

#### Step 4.3: Daily Automation (Set up once, runs automatically)

**Set up Windows Task Scheduler:**
   - Press `Win + R`, type `taskschd.msc`, press Enter
   - Click **"Create Basic Task..."**
   - **Name**: `M365 Daily Extraction`
   - **Trigger**: Daily at 8:00 AM
   - **Action**: Start a program
    - Program: `<path-to-ai-secretary>\SubstrateDataExtraction\run_daily.bat`
   - Click **Finish**

  If you move the main repository, update the scheduled task path to the new `SubstrateDataExtraction\run_daily.bat` location.

**Result**: Data automatically extracted daily at 8:00 AM Beijing time!

#### Video Downloads

Videos in transcripts would be downloaded automatically, you can add an argument to disable video downloading:

```bash
python m365_extractor.py --downloadVideos false
```

---

## Real-Time Transcript Monitor (Optional)

A continuous service that checks for new meeting transcripts every 5 minutes and saves them as individual text files.

### What It Does

- Checks for new transcripts every 5 minutes
- Saves to `output/meetings/YYYY-MM-DD_meeting_name.txt`
- Filters out empty transcripts automatically
- Tracks processed transcripts to avoid duplicates

### How to Use

```powershell
# Start the monitor
.\start_monitor.bat

# Stop: Close the window or press Ctrl+C
```

**Output:** Individual transcript files in `output/meetings/` and logs in `logs/transcript_monitor.log`

---

## Common Issues & Solutions

| Issue | Solution |
|-------|----------|
| `python: command not found` | Run `.\install_python.ps1` or manually install Python |
| `git: command not found` | Download from https://git-scm.com/download/win |
| `No module named 'requests'` | Run: `pip install requests msal PyJWT` |
| `Collaborators file not found` | Run setup first: `python m365_extractor.py --setup` |
| `401 Unauthorized` | Delete the `token_cache` folder and re-run |
| `Data for [date] already exists` | Delete existing files or run backfill for different dates |
| No daily data extracted | Check Task Scheduler: "M365 Daily Extraction" - run manually to test |
| Backfill failed on some days | Check logs in `logs/` folder - backfill has retry logic (3 attempts) |

---

## Need Help?

**For setup issues:**
1. Check the [Common Issues & Solutions](#common-issues--solutions) section
2. Verify you followed all 3 steps in [Daily Automation Setup](#step-4-daily-automation-setup)

**For automation issues:**
1. Check Task Scheduler is enabled and has permissions
2. View logs in `logs/` folder for errors
3. Run manually to test: `python m365_extractor.py --daily`

**Opening an issue:**
Include:
- Error message (full copy-paste)
- Step where error occurred
- Python version: `python --version`
- OS: Windows 10/11
