"""
install_extension_playwright.py
================================
Installs the AI Secretary browser extension into Microsoft Edge permanently.

How it works:
  1. Kill Edge, then launch it via Playwright (persistent Default profile)
  2. Open edge://extensions and enable developer mode
  3. Click "Load unpacked" - native Windows folder picker dialog opens
  4. pywinauto/win32: type the extension path into the dialog edit box via
     WM_SETTEXT + Enter (navigates dialog to folder), then click Select Folder
  5. Close Edge
  6. Verify by checking Secure Preferences directly (Playwright adds
     --disable-extensions so management.getAll() always returns [] -- the
     Secure Preferences file is the ground truth)

Note: On subsequent runs the dialog already remembers the browser_extension path.
"""

import ctypes
import json
import os
import subprocess
import sys
import time
import threading
from pathlib import Path

try:
    from playwright.sync_api import sync_playwright
except ImportError:
    print("Run: pip install playwright && playwright install msedge")
    sys.exit(1)

try:
    import win32gui
    import win32con
    import win32api
except ImportError:
    print("Run: pip install pywin32")
    sys.exit(1)

# Config
SCRIPT_DIR    = Path(__file__).parent
EXT_DIR       = (SCRIPT_DIR.parent / "browser_extension").resolve()
USER_DATA_DIR = Path(os.environ["LOCALAPPDATA"]) / "Microsoft" / "Edge" / "User Data"
SECURE_PREFS  = USER_DATA_DIR / "Default" / "Secure Preferences"
EXT_ID        = "mninbaioclkdbmeppcipanjiiiegbgdd"


def kill_edge():
    subprocess.run(["taskkill", "/IM", "msedge.exe", "/F"], capture_output=True)
    time.sleep(0.8)


def find_folder_dialog(timeout=12):
    """Return HWND of the 'Select the extension directory.' dialog, or None."""
    deadline = time.time() + timeout

    def scan(result):
        def _top(hwnd, extra):
            try:
                if (win32gui.GetClassName(hwnd) == "#32770"
                        and "extension" in win32gui.GetWindowText(hwnd).lower()):
                    extra.append(hwnd)
            except Exception:
                pass
        win32gui.EnumWindows(_top, result)
        def _child(hwnd, extra):
            try: win32gui.EnumChildWindows(hwnd, _top, extra)
            except Exception: pass
        win32gui.EnumWindows(_child, result)

    while time.time() < deadline:
        found = []
        scan(found)
        if found:
            return found[0]
        time.sleep(0.4)
    return None


def handle_folder_dialog(ext_path, timeout=12):
    """Find dialog, type path via WM_SETTEXT+Enter, click Select Folder."""
    dlg_hwnd = find_folder_dialog(timeout)
    if dlg_hwnd is None:
        print("  [!] Folder dialog did not appear")
        return

    print(f"  Dialog: {win32gui.GetWindowText(dlg_hwnd)!r}")

    def show_addr(hwnd, _):
        try:
            if win32gui.GetClassName(hwnd) == "ToolbarWindow32":
                t = win32gui.GetWindowText(hwnd)
                if "Address:" in t: print(f"  {t!r}")
        except Exception: pass
    win32gui.EnumChildWindows(dlg_hwnd, show_addr, None)

    # Find widest Edit control (the Folder: path input)
    import ctypes as _ct
    class RECT(_ct.Structure):
        _fields_ = [("left", _ct.c_long), ("top", _ct.c_long),
                    ("right", _ct.c_long), ("bottom", _ct.c_long)]
    edits = []
    def find_edits(hwnd, _):
        try:
            if win32gui.GetClassName(hwnd).lower() == "edit":
                r = RECT()
                _ct.windll.user32.GetWindowRect(hwnd, _ct.byref(r))
                edits.append((r.right - r.left, hwnd))
        except Exception: pass
    win32gui.EnumChildWindows(dlg_hwnd, find_edits, None)

    if edits:
        edits.sort(reverse=True)
        edit_hwnd = edits[0][1]
        # Type path + Enter to navigate dialog
        win32gui.SendMessage(edit_hwnd, win32con.WM_SETTEXT, 0, ext_path)
        time.sleep(0.1)
        win32gui.SendMessage(edit_hwnd, win32con.WM_KEYDOWN, win32con.VK_RETURN, 1)
        win32gui.SendMessage(edit_hwnd, win32con.WM_KEYUP, win32con.VK_RETURN, 0xC0000001)
        time.sleep(1.2)
        # Show updated address bar
        win32gui.EnumChildWindows(dlg_hwnd, show_addr, None)
    else:
        print("  [!] No Edit control found in dialog")

    # Click Select Folder button
    btns = []
    def find_select(hwnd, _):
        try:
            if (win32gui.GetClassName(hwnd).lower() == "button"
                    and "select" in win32gui.GetWindowText(hwnd).lower()):
                btns.append(hwnd)
        except Exception: pass
    win32gui.EnumChildWindows(dlg_hwnd, find_select, None)

    if btns:
        win32gui.SendMessage(btns[0], win32con.BM_CLICK, 0, 0)
        print("  'Select Folder' clicked")
    else:
        win32gui.SendMessage(dlg_hwnd, win32con.WM_KEYDOWN, win32con.VK_RETURN, 1)
        print("  Pressed Enter on dialog (Select Folder button not found)")


def check_installed():
    """Return extension entry from Secure Preferences, or None."""
    try:
        prefs = json.loads(SECURE_PREFS.read_text(encoding="utf-8"))
        return prefs.get("extensions", {}).get("settings", {}).get(EXT_ID)
    except Exception:
        return None


def install_extension():
    if not EXT_DIR.exists():
        print(f"[!] Extension folder not found: {EXT_DIR}")
        sys.exit(1)

    print()
    print("=" * 62)
    print("  AI Secretary Extension Installer")
    print("=" * 62)
    print(f"\n  Extension : {EXT_DIR}")
    print(f"  Profile   : {USER_DATA_DIR / 'Default'}")

    entry_before = check_installed()
    if entry_before:
        print(f"\n  Current state: location={entry_before.get('location')}, state={entry_before.get('state')}")
    else:
        print("\n  Extension not yet in Secure Preferences")

    print()
    kill_edge()
    print("Closed Edge.\n")

    with sync_playwright() as p:
        print("Launching Edge via Playwright...")
        print("  NOTE: Playwright disables extensions in this session.")
        print("        Installation is verified via Secure Preferences.\n")
        browser = p.chromium.launch_persistent_context(
            user_data_dir=str(USER_DATA_DIR),
            channel="msedge",
            headless=False,
            args=["--profile-directory=Default"],
        )
        page = browser.pages[0] if browser.pages else browser.new_page()

        print("Opening edge://extensions ...")
        page.goto("edge://extensions/")
        page.wait_for_timeout(800)

        print("Enabling developer mode...")
        page.evaluate("""
            () => new Promise(r =>
                chrome.developerPrivate.updateProfileConfiguration(
                    { inDeveloperMode: true }, () => r()
                )
            )
        """)
        page.reload()
        page.wait_for_timeout(1000)
        print("Developer mode enabled.\n")

        t = threading.Thread(target=handle_folder_dialog, args=(str(EXT_DIR),), daemon=True)
        t.start()
        print("Clicking 'Load unpacked'...")
        page.locator("text=Load unpacked").click()
        t.join(timeout=20)
        print()

        page.wait_for_timeout(1200)
        browser.close()

    print("Edge closed.\n")
    time.sleep(0.3)

    entry_after = check_installed()
    print("=" * 62)
    if entry_after and entry_after.get("location") in (1, 4):
        print("  SUCCESS: AI Secretary installed!")
        print(f"    location = {entry_after.get('location')} (permanent)")
        print(f"    state    = {entry_after.get('state')}")
        print(f"    path     = {entry_after.get('path', '')}")
        print()
        print("  Open Edge normally to use the extension.")
    elif entry_after and entry_after.get("location") == 8:
        print("  PARTIAL: Location=8 (command-line only).")
        print("    Re-run and check dialog selects browser_extension folder.")
    else:
        print("  FAILED: Extension not found in Secure Preferences.")
    print("=" * 62)
    print()


if __name__ == "__main__":
    try:
        install_extension()
    except KeyboardInterrupt:
        print("\nCancelled.")
    except Exception as e:
        import traceback
        print(f"\n[Error] {e}")
        traceback.print_exc()
        sys.exit(1)
