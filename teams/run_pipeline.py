import sys
import subprocess
from pathlib import Path
from datetime import datetime, timedelta
import json

def run_script(script_name, args=None):
    """Runs a python script and returns True if successful."""
    command = [sys.executable, script_name]
    if args:
        command.extend(args)
    
    print(f"\n[EXEC] Running {script_name}...")
    try:
        result = subprocess.run(command, check=True, capture_output=False)
        return result.returncode == 0
    except subprocess.CalledProcessError as e:
        print(f"[ERROR] Script {script_name} failed with return code {e.returncode}")
        return False
    except Exception as e:
        print(f"[ERROR] Failed to run {script_name}: {e}")
        return False

def check_file_freshness(file_path, max_age_minutes=10):
    """Checks if a file exists and is fresher than max_age_minutes."""
    path = Path(file_path)
    if not path.exists():
        return False
    
    mtime = datetime.fromtimestamp(path.stat().st_mtime)
    age = datetime.now() - mtime
    return age < timedelta(minutes=max_age_minutes)

def main():
    print("=" * 80)
    print("TEAMS MESSAGE PROCESSING PIPELINE")
    print("=" * 80)

    # Determine target user
    if len(sys.argv) < 2:
        print("Usage: python run_pipeline.py <username_or_email> [--max <max_conversations>]")
        print("Example: python run_pipeline.py <username> --max 5")
        sys.exit(1)

    input_user = sys.argv[1]
    if "@" in input_user:
        target_email = input_user
    else:
        target_email = f"{input_user}@microsoft.com"
    
    # Parse optional max argument
    max_conversations = None
    if len(sys.argv) >= 4 and sys.argv[2] == "--max":
        try:
            max_conversations = sys.argv[3]
        except IndexError:
            pass
    
    print(f"[CONFIG] Target User: {target_email}")
    if max_conversations:
        print(f"[CONFIG] Max Conversations: {max_conversations}")

    # Setup paths
    base_dir = Path(__file__).parent
    repo_root = base_dir.parent
    input_dir = base_dir / "input"
    all_messages_file = input_dir / "all_teams_messages.json"
    # Skip rules now live in repo-root pipeline_config.json.
    # Keep legacy teams/config.json optional for backward compatibility.
    config_file = base_dir / "config.json"
    output_conversations_dir = base_dir / "output" / "teams_conversations"
    output_analysis_dir = base_dir / "output" / "teams_analysis"
    guide_file = base_dir / "Teams_Chat.md"
    
    # Strict dataset-scoped profile: resolve active dataset folder from pipeline config.
    def _load_pipeline_config() -> dict:
        cfg: dict = {}
        for p in (repo_root / 'pipeline_config.json', repo_root / 'pipeline_config.user.json'):
            try:
                if p.exists() and p.is_file():
                    with p.open('r', encoding='utf-8') as f:
                        data = json.load(f)
                    if isinstance(data, dict):
                        cfg.update(data)
            except Exception:
                continue
        return cfg

    def _resolve_active_data_folder(cfg: dict) -> Path:
        chosen = cfg.get('active_data_folder_path')
        if not isinstance(chosen, str) or not chosen.strip():
            chosen = cfg.get('data_folder_path')
        if not isinstance(chosen, str) or not chosen.strip():
            chosen = 'incremental_data'
        p = Path(chosen)
        if not p.is_absolute():
            p = (repo_root / p).resolve()
        return p

    cfg = _load_pipeline_config()
    inc_dir = _resolve_active_data_folder(cfg)
    user_profile_path = inc_dir / 'user_profile.json'
    if not user_profile_path.exists():
        print(f"[ERROR] Dataset user_profile.json not found: {user_profile_path}")
        print("[HINT] Create it in the active dataset or use the app's Refetch Profile.")
        sys.exit(1)

    # Step 1: Check for all_teams_messages.json
    print("\n[STEP 1] Checking for all_teams_messages.json...")
    if not all_messages_file.exists():
        print(f"[ERROR] {all_messages_file} not found. Aborting.")
        sys.exit(1)
    print("[SUCCESS] all_teams_messages.json found.")

    # Step 2: Process Teams Messages
    print("\n[STEP 2] Processing Teams messages...")
    # process_teams_messages.py <input_json> <output_dir> [config_file]
    # Prefer pipeline_config.json; pass legacy config only if it exists.
    cmd_args = [str(all_messages_file), str(output_conversations_dir)]
    if config_file.exists():
        cmd_args.append(str(config_file))

    if not run_script(str(base_dir / "process_teams_messages.py"), cmd_args):
        print("[ERROR] Failed to process teams messages. Aborting.")
        sys.exit(1)
    print("[SUCCESS] Teams messages processed.")

    # Step 3: AI Analysis
    print("\n[STEP 3] Running AI Analysis...")
    # analyze_teams_conversations.py <conversations_dir> --user <email> --guide <guide_path> --output <output_dir> [--max <max>]
    
    analysis_cmd = [
        "-m",
        "teams.analyze_teams_conversations",
        str(output_conversations_dir),
        "--user", target_email,
        "--guide", str(guide_file),
        "--output", str(output_analysis_dir)
    ]
    
    analysis_cmd.extend(["--profile", str(user_profile_path)])
    
    if max_conversations:
        analysis_cmd.extend(["--max", max_conversations])
    
    if not run_script(analysis_cmd[0], analysis_cmd[1:]):
        print("[ERROR] AI Analysis failed. Aborting.")
        sys.exit(1)

    # Step 4: Deduplicate Todos
    print("\n[STEP 4] Deduplicating todos...")
    user_id = target_email.split("@")[0]
    summary_file = output_analysis_dir / f"teams_analysis_summary_{user_id}.json"
    if summary_file.exists():
        if not run_script("-m", [
            "teams.dedup_todos",
            "--input", str(summary_file),
            "--output", str(summary_file),
            "--conversations-dir", str(output_conversations_dir),
        ]):
            print("[ERROR] Todo dedup failed. Aborting.")
            sys.exit(1)
        print("[SUCCESS] Todos deduplicated.")
    else:
        print(f"[WARN] Summary file not found for todo dedup: {summary_file}")
    
    print("\n" + "=" * 80)
    print("[COMPLETE] Pipeline finished successfully!")
    print(f"Analysis results available in: {output_analysis_dir}")
    print("=" * 80)

if __name__ == "__main__":
    main()
