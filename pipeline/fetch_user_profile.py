import sys
import subprocess
import json
from pathlib import Path
import argparse
import shutil
import importlib.util


REPO_NAME = "SubstrateDataExtraction"


class Colors:
    CYAN = '\033[96m'
    YELLOW = '\033[93m'
    RED = '\033[91m'
    BOLD = '\033[1m'
    ENDC = '\033[0m'


def run_command(command, cwd=None, description=None):
    """Runs a command and prints status."""
    if description:
        print(f"\n[STEP] {description}")

    cmd_str = ' '.join(command) if isinstance(command, list) else command
    print(f"Running: {cmd_str}")

    try:
        subprocess.check_call(command, cwd=cwd)
        print("[OK] Success")
        return True
    except subprocess.CalledProcessError as e:
        print(f"[ERROR] Command failed with exit code {e.returncode}")
        return False


def _print_profile_table(profile: dict) -> None:
    print("\n" + "=" * 80)
    print(f"{Colors.BOLD}{'FIELD':<25} | {'VALUE'}{Colors.ENDC}")
    print("-" * 80)
    for key, value in profile.items():
        readable_key = str(key).replace('_', ' ').title()
        if isinstance(value, list):
            val_str = ", ".join(str(v) for v in value) if value else "-"
        else:
            val_str = str(value)
        print(f"{Colors.CYAN}{readable_key:<25}{Colors.ENDC} | {Colors.YELLOW}{val_str}{Colors.ENDC}")
    print("=" * 80)


def _show_profile(final_profile_path: Path) -> None:
    try:
        with open(final_profile_path, 'r', encoding='utf-8') as f:
            profile = json.load(f)
        _print_profile_table(profile)
    except Exception as e:
        print(f"{Colors.RED}[ERROR] Failed to read profile: {e}{Colors.ENDC}")


def _ensure_substrate_repo(target_dir: Path) -> None:
    if target_dir.exists():
        print(f"\n[INFO] Found vendored {REPO_NAME} at {target_dir}")
    else:
        raise RuntimeError(
            f"Missing vendored {REPO_NAME} at {target_dir}. "
            "Restore the folder from this repository before running profile fetch."
        )

    req_file = target_dir / "requirements.txt"
    if req_file.exists():
        required_modules = ("requests", "msal", "jwt", "playwright")
        missing_modules = [name for name in required_modules if importlib.util.find_spec(name) is None]
        if missing_modules:
            print(f"\n[INFO] Missing SubstrateDataExtraction modules detected: {', '.join(missing_modules)}")
            ok = run_command(
                [sys.executable, "-m", "pip", "install", "-r", str(req_file)],
                description="Installing SubstrateDataExtraction dependencies (best-effort)",
            )
            if not ok:
                print("[WARNING] Failed to install some SubstrateDataExtraction dependencies.")
        else:
            print("\n[INFO] SubstrateDataExtraction dependencies already available. Skipping pip install.")
    else:
        print(f"\n[INFO] No requirements.txt found at {req_file}. Skipping pip install.")


def _build_structured_profile(substrate_profile_path: Path) -> dict:
    with open(substrate_profile_path, 'r', encoding='utf-8') as f:
        data = json.load(f)

    formatted = data.get("formatted_profile", {})
    alias = data.get("alias", "")
    upn = data.get("upn", "")

    phones = formatted.get("phones", [])
    phone_numbers = [p.get("Number") for p in phones if isinstance(p, dict) and p.get("Number")]

    return {
        "USER_NAME": [formatted.get("Name", "")] if formatted.get("Name") else [],
        "USER_ALIAS": [alias] if alias else [],
        "USER_OFFICE_LOCATION": [formatted.get("officeLocation", "")] if formatted.get("officeLocation") else [],
        "USER_OFFICE_CITY": [formatted.get("Office location", "")] if formatted.get("Office location") else [],
        "USER_JOB_TITLE": [formatted.get("Job title", "")] if formatted.get("Job title") else [],
        "USER_COMPANY": [formatted.get("companyName", "")] if formatted.get("companyName") else [],
        "USER_EMAIL": [upn] if upn else [],
        "USER_TEAM": [formatted.get("department", "")] if formatted.get("department") else [],
        "USER_TELEPHONE": phone_numbers,
        "MANAGER_INFO": [formatted.get("Manager", "")] if formatted.get("Manager") else [],
        "SKIP_MANAGER_INFO": [formatted.get("Skip manager", "")] if formatted.get("Skip manager") else [],
        "KEY_TEAMMATES_INFO": [],
        "following": [],
    }


def _load_json_best_effort(path: Path) -> dict:
    try:
        if not path.exists():
            return {}
        with open(path, 'r', encoding='utf-8-sig') as f:
            data = json.load(f)
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def _merge_profiles_preserving_user_fields(existing: dict, fetched: dict) -> dict:
    """Merge fetched baseline fields into an existing profile.

    We preserve user-edited fields (like following)
    so a refetch doesn't wipe user customization.
    """
    if not isinstance(existing, dict):
        existing = {}
    if not isinstance(fetched, dict):
        fetched = {}

    out = dict(existing)

    preserve_keys = {'following'}
    for k, v in fetched.items():
        if k in preserve_keys:
            continue
        out[k] = v

    # Ensure baseline keys exist if missing.
    for k in ('USER_NAME', 'USER_ALIAS', 'USER_EMAIL', 'USER_JOB_TITLE', 'USER_COMPANY', 'USER_TELEPHONE'):
        if k not in out and k in fetched:
            out[k] = fetched[k]

    # Preserve existing following if present; otherwise take fetched defaults.
    for k in ('following',):
        if k not in out and k in fetched:
            out[k] = fetched[k]

    # Hard deprecation: identity should not persist in user_profile.json.
    if 'identity' in out:
        out.pop('identity', None)

    # Hard deprecation: active_projects should not persist in user_profile.json.
    if 'active_projects' in out:
        out.pop('active_projects', None)

    return out


def main() -> None:
    parser = argparse.ArgumentParser(description="AI Secretary: Fetch/Create user_profile.json")
    parser.add_argument("--user-profile", help="Path to an existing user_profile.json to copy into place")
    parser.add_argument(
        "--output-profile",
        help="Path to write the structured user_profile.json (default: repo-root user_profile.json)",
    )
    parser.add_argument("--force", action="store_true", help="Force re-fetch even if user_profile.json exists")
    args = parser.parse_args()

    base_dir = Path(__file__).resolve().parent.parent
    target_dir = base_dir / REPO_NAME
    if args.output_profile:
        final_profile_path = Path(args.output_profile).expanduser()
    else:
        # Strict default: never generate repo-root user_profile.json.
        # Prefer the configured active dataset folder; otherwise require --output-profile.
        cfg: dict = {}
        for p in (base_dir / 'config' / 'pipeline_config.json', base_dir / 'config' / 'pipeline_config.user.json'):
            try:
                if p.exists() and p.is_file():
                    with p.open('r', encoding='utf-8') as f:
                        data = json.load(f)
                    if isinstance(data, dict):
                        cfg.update(data)
            except Exception:
                continue

        chosen = cfg.get('active_data_folder_path')
        if not isinstance(chosen, str) or not chosen.strip():
            chosen = cfg.get('data_folder_path')
        if not isinstance(chosen, str) or not chosen.strip():
            raise SystemExit(
                "[ERROR] Refusing to write repo-root user_profile.json. "
                "Set active_data_folder_path in pipeline_config.user.json or pass --output-profile <dataset>/user_profile.json"
            )

        inc_dir = Path(chosen)
        if not inc_dir.is_absolute():
            inc_dir = (base_dir / inc_dir).resolve()
        final_profile_path = inc_dir / 'user_profile.json'
    try:
        final_profile_path.parent.mkdir(parents=True, exist_ok=True)
    except Exception:
        pass

    print("=" * 60)
    print("USER PROFILE SETUP")
    print("=" * 60)

    # 1) Copy in an existing profile if provided
    if args.user_profile:
        p_path = Path(args.user_profile)
        if not p_path.exists():
            raise SystemExit(f"[ERROR] Provided profile not found: {p_path}")
        print(f"[INFO] Using provided user profile: {p_path}")
        if p_path.resolve() == final_profile_path.resolve():
            print("[INFO] Provided profile is already in place. Skipping copy.")
        else:
            shutil.copy2(str(p_path), str(final_profile_path))

    # 2) If profile exists and not forcing, show it and exit
    existing_profile: dict | None = None
    if final_profile_path.exists():
        if not args.force:
            print(f"\n[INFO] User profile found at {final_profile_path}.")
            _show_profile(final_profile_path)
            print("\n" + "=" * 60)
            print("[SUCCESS] User profile setup complete!")
            print("=" * 60)
            return

        print(f"\n[INFO] user_profile.json exists; forcing re-fetch and updating baseline fields.")
        existing_profile = _load_json_best_effort(final_profile_path)

    # 3) Otherwise, fetch profile via SubstrateDataExtraction
    _ensure_substrate_repo(target_dir)

    profile_script = target_dir / "get_user_profile.py"
    if not profile_script.exists():
        raise SystemExit(f"[ERROR] Script not found: {profile_script}")

    if not run_command([sys.executable, str(profile_script)], cwd=target_dir, description="Fetching user profile"):
        raise SystemExit(1)

    src = target_dir / "output" / "user_profile.json"
    if not src.exists():
        raise SystemExit(f"[ERROR] Expected output file not found: {src}")

    substrate_profile_path = base_dir / "user_profile_substrate.json"
    shutil.copy2(str(src), str(substrate_profile_path))
    print(f"[INFO] Copied raw profile to {substrate_profile_path}")

    new_profile = _build_structured_profile(substrate_profile_path)
    final_profile = new_profile
    if isinstance(existing_profile, dict) and existing_profile:
        final_profile = _merge_profiles_preserving_user_fields(existing_profile, new_profile)

    with open(final_profile_path, 'w', encoding='utf-8') as f:
        json.dump(final_profile, f, indent=2, ensure_ascii=False)
    print(f"[INFO] Wrote structured profile at {final_profile_path}")

    _show_profile(final_profile_path)

    print("\n" + "=" * 60)
    print("[SUCCESS] User profile setup complete!")
    print("=" * 60)


if __name__ == "__main__":
    main()
