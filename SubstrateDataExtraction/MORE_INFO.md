# SubstrateDataExtract - Setup for New User

## You received this project folder? Start here!

---

## STEP 1: Install Python Packages (30 seconds)

Open PowerShell or Command Prompt **IN THIS FOLDER** and run:

```powershell
pip install requests msal PyJWT
```

Done? Move to Step 2.

---

## STEP 2: Test if It Works (10 seconds)

Run this command:

```powershell
python main.py --help
```

**Did you see a help menu?** Great! Skip to Step 4.

**Did you see an error?** Continue to Step 3.

---

## STEP 3: Fix "No module named 'src'" Error

This error means Python can't find the project files. It happens when folders are copied.

### Fix: Create the missing __init__.py files

Copy and paste these commands (all at once):

```powershell
echo. > src\__init__.py
echo. > src\auth\__init__.py
echo. > src\client\__init__.py
echo. > src\services\__init__.py
echo. > src\utils\__init__.py
```

Now try again:
```powershell
python main.py --help
```

**Still not working?** Make sure you're in the correct folder. Type `dir` and you should see `main.py` in the list.

---

## STEP 4: Run Your First Command!

Try fetching 10 Teams messages:

```powershell
python main.py teams --top 10
```

A browser window will open asking you to sign in. This is normal - sign in with your Microsoft account.

After signing in, the command will finish and save results to `output/teams_messages.json`.

---

## Common Commands You Can Use

```powershell
# Get Teams messages from last 30 days
python fetch_all_teams_messages.py --days 30

# Get all emails from last week
python fetch_all_emails.py --days 7

# Get unread emails
python main.py email --filter "IsRead eq false" --top 50

# Get calendar events
python main.py calendar --start 2025-01-01 --end 2025-01-31
```

---

## Where's My Data?

Check the `output/` folder. All JSON files are saved there.

---

## Need More Help?

See the detailed guides:
- `SETUP_GUIDE.md` - Full setup instructions and troubleshooting
- `EMAIL_EXTRACTION_GUIDE.md` - Everything about extracting emails

---

**Questions? Ask the person who shared this with you!**
