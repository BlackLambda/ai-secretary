"""
Process Teams chat conversations with AI analysis to extract tasks and recommendations.
Reads conversation JSON files and uses Azure OpenAI to analyze each one.
"""

import json
import sys
import os
import shutil
from pathlib import Path
from datetime import datetime, timezone

YELLOW = "\033[93m"
RESET = "\033[0m"

from lib.ai_utils import get_azure_openai_client, DEPLOYMENT_NAME, AZURE_OPENAI_TIMEOUT_SECONDS, ai_chat_json
from lib.pipeline_config_manager import ensure_effective_config
from ai_secretary_core.paths import RepoPaths
from lib.ai_utils import drop_items_with_past_deadlines, summarize_deadline_drop
from ai_secretary_core.recent_focus import default_recent_focus_path, resolve_effective_active_projects


def _norm_match_text(text: str) -> str:
    if not isinstance(text, str):
        return ""
    # Normalize whitespace and case to make substring matching robust.
    return " ".join(text.replace("\r", "\n").split()).strip().lower()


def _find_quote_timestamp(messages: list, quote: str) -> str | None:
    """Best-effort: find the message timestamp that contains the quote."""
    q = _norm_match_text(quote)
    if len(q) < 5:
        return None

    # Try full quote, then a shorter prefix if needed.
    candidates = [q]
    if len(q) > 120:
        candidates.append(q[:120])
    if len(q) > 80:
        candidates.append(q[:80])

    for cand in candidates:
        for msg in messages or []:
            if not isinstance(msg, dict):
                continue
            ts = msg.get("timestamp")
            if not isinstance(ts, str) or not ts.strip():
                continue
            content = _norm_match_text(msg.get("content") or "")
            subject = _norm_match_text(msg.get("subject") or "")
            haystack = content + (" " + subject if subject else "")
            if cand and cand in haystack:
                return ts.strip()
    return None


def _item_text(item) -> str:
    if not isinstance(item, dict):
        return ""
    return (item.get("description") or item.get("task") or "").strip()


def _log_removed_items(prefix: str, removed_items) -> None:
    if not isinstance(removed_items, list) or not removed_items:
        return
    for r in removed_items:
        if not isinstance(r, dict):
            continue
        text = _item_text(r)
        if not text:
            continue
        reason = (r.get("removal_reason") or r.get("reason") or "").strip()
        evidence = (r.get("evidence") or r.get("supporting_quote") or "").strip()
        if reason and evidence:
            print(f"  {YELLOW}[REMOVED] {prefix}: {text} — {reason} (evidence: {evidence}){RESET}")
        elif reason:
            print(f"  {YELLOW}[REMOVED] {prefix}: {text} — {reason}{RESET}")
        else:
            print(f"  {YELLOW}[REMOVED] {prefix}: {text}{RESET}")


def _load_json_file(path: Path):
    try:
        if path.exists():
            with open(path, 'r', encoding='utf-8') as f:
                return json.load(f)
    except Exception as e:
        print(f"[WARN] Failed to load JSON {path}: {e}")
    return None


def _load_pipeline_scoring_system(project_root_dir: Path):
    """Load Teams scoring rubric from pipeline_config.json (optional)."""
    cfg = None
    try:
        cfg = ensure_effective_config(project_root_dir)
    except Exception:
        cfg = None
    scoring_path = RepoPaths(project_root_dir).teams_scoring_system_default()
    if isinstance(cfg, dict):
        cfg_path = cfg.get("scoring_system_teams_path")
        if isinstance(cfg_path, str) and cfg_path.strip():
            scoring_path = project_root_dir / cfg_path
    scoring = _load_json_file(scoring_path)
    return scoring, scoring_path


def _infer_project_root_dir(config_path: Path | None, guide_path: Path | None) -> Path:
    """Infer the app/repo root directory.

    In both dev and installed layouts, this script typically lives under:
      <project_root>/teams/analyze_teams_conversations.py
    and related config lives under:
      <project_root>/pipeline_config*.json
    """

    candidates: list[Path] = []

    for p in (config_path, guide_path):
        if p is None:
            continue
        try:
            p = Path(p).resolve()
        except Exception:
            p = Path(p)
        # If p is .../<root>/teams/<file>, then root is two levels up.
        if p.parent.name.lower() == "teams" and len(p.parents) >= 2:
            candidates.append(p.parents[1])
        # Fallback: one level up from the provided path.
        candidates.append(p.parent)

    # Script location: .../<root>/teams/analyze_teams_conversations.py
    try:
        candidates.append(Path(__file__).resolve().parents[1])
    except Exception:
        pass

    # Last resort: current working directory
    candidates.append(Path.cwd())

    def looks_like_root(path: Path) -> bool:
        return (
            (path / "pipeline_config.default.json").exists()
            or (path / "run_incremental_pipeline.py").exists()
            or (path / "pipeline_config_manager.py").exists()
            or (path / "teams").exists()
        )

    for cand in candidates:
        if cand and cand.exists() and cand.is_dir() and looks_like_root(cand):
            return cand

    # Best-effort: return first directory-ish candidate.
    for cand in candidates:
        if cand and cand.exists() and cand.is_dir():
            return cand

    return Path.cwd()


class TeamsConversationAnalyzer:
    """Analyzes Teams conversations using Azure OpenAI."""
    
    def __init__(
        self,
        guide_path,
        target_user_email,
        output_dir="output/teams_analysis",
        config_path=None,
        user_profile=None,
        existing_summary_path=None,
        recent_focus_path: Path | None = None,
    ):
        """
        Initialize the analyzer.
        
        Args:
            guide_path: Path to the Teams_Chat.md guide
            target_user_email: Email of target user to analyze for
            output_dir: Directory to save analysis results
            config_path: Path to config.json
            user_profile: Dictionary containing user profile data
            existing_summary_path: Path to existing summary JSON file to merge with
        """
        self.guide_path = Path(guide_path)
        self.target_user_email = target_user_email
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.user_profile = user_profile or {}
        self.recent_focus_path = recent_focus_path
        
        # Load existing summary
        self.existing_summary = []
        if existing_summary_path:
            path = Path(existing_summary_path)
            if path.exists():
                print(f"[LOADING] Existing summary: {path}")
                try:
                    with open(path, 'r', encoding='utf-8') as f:
                        data = json.load(f)
                        if isinstance(data, list):
                            self.existing_summary = data
                        else:
                            print("[WARN] Existing summary is not a list, ignoring.")
                except Exception as e:
                    print(f"[WARN] Failed to load existing summary: {e}")

        # Load config
        self.skip_names = []
        self.skip_ids = []
        self.skip_self_assigned_tasks = False
        resolved_config_path: Path | None = None
        if config_path:
            config_path = Path(config_path)
            resolved_config_path = config_path
            if config_path.exists():
                print(f"[LOADING] Config: {config_path}")
                with open(config_path, 'r', encoding='utf-8') as f:
                    config = json.load(f)
                    self.skip_names = config.get('skip_names', [])
                    self.skip_ids = config.get('skip_conversation_ids', [])
                    self.skip_self_assigned_tasks = config.get('skip_self_assigned_tasks', False)
            else:
                print(f"[WARNING] Config file not found: {config_path}")

        # Load guide
        print(f"[LOADING] Analysis guide: {self.guide_path}")
        with open(self.guide_path, 'r', encoding='utf-8') as f:
            raw_guide = f.read()
            guide = self._replace_placeholders(raw_guide)

        project_root_dir = _infer_project_root_dir(resolved_config_path, self.guide_path)
        scoring_system, scoring_path = _load_pipeline_scoring_system(project_root_dir)
        if isinstance(scoring_system, dict):
            guide += (
                "\n\nUSER-CUSTOMIZABLE SCORING RUBRIC (authoritative):\n"
                + json.dumps(scoring_system, indent=2)
                + "\n\nUse this rubric to produce priority_score and scoring_breakdown.\n"
            )
            print(f"[LOADING] Teams scoring rubric: {scoring_path}")

        # Inject target-user context into the system prompt so the model
        # has more grounding when extracting tasks/actions.
        try:
            rf_path = self.recent_focus_path or default_recent_focus_path(project_root_dir)
            active_projects = resolve_effective_active_projects(recent_focus_path=rf_path, user_profile=self.user_profile)
            guide += (
                "\n\nTARGET USER PROFILE (additional context):\n"
                f"Name: {(self.user_profile.get('USER_NAME', ['']) or [''])[0]}\n"
                f"Alias: {(self.user_profile.get('USER_ALIAS', ['']) or [''])[0]}\n"
                f"Email: {(self.user_profile.get('USER_EMAIL', ['']) or [''])[0] or self.target_user_email}\n"
                f"Manager: {(self.user_profile.get('MANAGER_INFO', ['']) or [''])[0]}\n"
                f"Team: {self.user_profile.get('USER_TEAM', [])}\n"
                f"Active Projects: {active_projects}\n"
                f"Following: {self.user_profile.get('following', [])}\n"
            )
        except Exception:
            pass

        self.guide_content = guide
        
        # Initialize Azure OpenAI client
        print("[INIT] Initializing Azure OpenAI client...")
        self.client = get_azure_openai_client()
        print("[OK] Azure OpenAI client initialized")
        
        # Results accumulator
        self.all_results = []
        self.skipped_conversations = []
    
    def _replace_placeholders(self, content):
        """Replace placeholders in guide content with user profile data."""
        
        # Default values if profile is missing
        user_name = "Target User"
        user_email = self.target_user_email
        user_alias = self.target_user_email.split('@')[0]
        
        # Extract from profile if available
        if self.user_profile:
            # Handle list values (take first item) or string values
            name_val = self.user_profile.get("USER_NAME", [])
            if isinstance(name_val, list) and name_val:
                user_name = name_val[0]
            elif isinstance(name_val, str):
                user_name = name_val
                
            alias_val = self.user_profile.get("USER_ALIAS", [])
            if isinstance(alias_val, list) and alias_val:
                user_alias = ", ".join(alias_val)
            elif isinstance(alias_val, str):
                user_alias = alias_val
                
            # Email usually matches target_user_email, but check profile too
            email_val = self.user_profile.get("USER_EMAIL", [])
            if isinstance(email_val, list) and email_val:
                user_email = email_val[0]
            elif isinstance(email_val, str):
                user_email = email_val

        # Perform replacements
        content = content.replace("{{USER_NAME}}", user_name)
        content = content.replace("{{USER_EMAIL}}", user_email)
        content = content.replace("{{USER_ALIAS}}", user_alias)
        
        return content

    def format_conversation_for_analysis(self, conversation_data):
        """Format conversation data into a readable text for AI analysis."""
        
        conv_text = f"""# Conversation Analysis Request

## Conversation Metadata
- **Conversation ID**: {conversation_data.get('conversation_id', 'N/A')}
- **Chat Name**: {conversation_data.get('chat_name', 'N/A')}
- **Participants Count**: {conversation_data.get('participantsCount', 0)}
- **Message Count**: {conversation_data.get('message_count', 0)}
- **Date Range**: {conversation_data.get('date_range', 'N/A')}

## Target User
- **Email**: {self.target_user_email}

## Participants
{', '.join(conversation_data.get('participants', [])[:20])}
{f"... and {len(conversation_data.get('participants', [])) - 20} more" if len(conversation_data.get('participants', [])) > 20 else ''}

## Conversation Messages

"""
        
        messages = conversation_data.get('messages', [])
        for idx, msg in enumerate(messages, 1):
            sender = msg.get('sender_name', 'Unknown')
            timestamp = msg.get('timestamp', '')
            content = msg.get('content', '(No content)')
            subject = msg.get('subject', '')
            has_attachments = msg.get('has_attachments', False)
            
            conv_text += f"\n### Message {idx}\n"
            conv_text += f"**From**: {sender}\n"
            conv_text += f"**Time**: {timestamp}\n"
            if subject:
                conv_text += f"**Subject**: {subject}\n"
            if has_attachments:
                conv_text += f"**Attachments**: Yes\n"
            conv_text += f"\n{content}\n"
            conv_text += "\n---\n"
        
        return conv_text
    
    def analyze_conversation(self, conversation_data, conv_filename):
        """
        Analyze a single conversation using Azure OpenAI.
        
        Args:
            conversation_data: Dictionary containing conversation data
            conv_filename: Filename of the conversation for reference
            
        Returns:
            Dictionary with analysis results
        """
        print(f"\n[ANALYZING] {conv_filename}")
        print(f"  Messages: {conversation_data.get('message_count', 0)}, "
              f"Participants: {conversation_data.get('participantsCount', 0)}")
        
        # Format conversation
        conversation_text = self.format_conversation_for_analysis(conversation_data)
        
        # Prepare messages for API
        messages = [
            {
                "role": "system",
                "content": self.guide_content
                + "\nIMPORTANT: Do not include tasks/actions whose deadline is already in the past.\n"
            },
            {
                "role": "user",
                "content": conversation_text
            }
        ]
        
        # Retry logic for connection errors
        max_retries = 3
        retry_delay = 2
        
        for attempt in range(max_retries):
            try:
                # Call Azure OpenAI
                analysis_result = ai_chat_json(self.client, messages)

                # Add timestamps to items
                current_time = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
                source_messages = conversation_data.get('messages', []) if isinstance(conversation_data, dict) else []

                for task in analysis_result.get('tasks', []):
                    task['last_updated'] = current_time
                    if 'scoring_evidence' not in task:
                        task['scoring_evidence'] = {}
                    if task.get('original_quote') and not task.get('original_quote_timestamp'):
                        ts = _find_quote_timestamp(source_messages, task.get('original_quote'))
                        if ts:
                            task['original_quote_timestamp'] = ts
                for action in analysis_result.get('recommended_actions', []):
                    action['last_updated'] = current_time
                    if 'scoring_evidence' not in action:
                        action['scoring_evidence'] = {}
                    if not action.get('original_quote'):
                        action['original_quote'] = (action.get('evidence') or action.get('supporting_quote') or '').strip()
                        if not action['original_quote']:
                            desc = (action.get('description') or action.get('task') or '').strip()
                            if desc:
                                print(f"  {YELLOW}[WARN] Recommended action missing original_quote: {desc[:80]}{RESET}")
                    if action.get('original_quote') and not action.get('original_quote_timestamp'):
                        ts = _find_quote_timestamp(source_messages, action.get('original_quote'))
                        if ts:
                            action['original_quote_timestamp'] = ts

                _log_removed_items("TASK", analysis_result.get("removed_tasks"))
                _log_removed_items("REC", analysis_result.get("removed_recommended_actions"))
                
                # Filter self-assigned tasks if configured
                if self.skip_self_assigned_tasks:
                    original_tasks = analysis_result.get('tasks', [])
                    filtered_tasks = []
                    for task in original_tasks:
                        assigned_to = task.get('assigned_to', '').strip()
                        assigned_by = task.get('assigned_by', '').strip()
                        
                        is_self_assigned = False
                        if assigned_to and assigned_by:
                            a_to = assigned_to.lower()
                            a_by = assigned_by.lower()
                            
                            if a_to == a_by:
                                is_self_assigned = True
                            elif "self-assigned" in a_by:
                                is_self_assigned = True
                            elif len(a_to) > 3 and len(a_by) > 3 and (a_to in a_by or a_by in a_to):
                                is_self_assigned = True
                        
                        if is_self_assigned:
                            print(f"  {YELLOW}[FILTER] Dropping self-assigned task: {task.get('description')[:50]}... ({assigned_to} by {assigned_by}){RESET}")
                        else:
                            filtered_tasks.append(task)
                    
                    analysis_result['tasks'] = filtered_tasks

                # Hard rule: drop expired deadlines even if the model outputs them.
                now = datetime.now(timezone.utc)
                kept_tasks, dropped_tasks = drop_items_with_past_deadlines(analysis_result.get('tasks', []), now=now)
                if dropped_tasks:
                    print(f"  {YELLOW}[FILTER] Dropped {len(dropped_tasks)} past-due tasks by deadline{RESET}")
                    for d in dropped_tasks[:10]:
                        print(f"    {YELLOW}- {summarize_deadline_drop(d)}{RESET}")
                analysis_result['tasks'] = kept_tasks

                kept_actions, dropped_actions = drop_items_with_past_deadlines(analysis_result.get('recommended_actions', []), now=now)
                if dropped_actions:
                    print(f"  {YELLOW}[FILTER] Dropped {len(dropped_actions)} past-due recommended actions by deadline{RESET}")
                    for d in dropped_actions[:10]:
                        print(f"    {YELLOW}- {summarize_deadline_drop(d)}{RESET}")
                analysis_result['recommended_actions'] = kept_actions

                # Add metadata
                analysis_result['conversation_file'] = conv_filename
                analysis_result['conversation_id'] = conversation_data.get('conversation_id', '')
                analysis_result['chat_name'] = conversation_data.get('chat_name', '')
                analysis_result['analyzed_at'] = datetime.now().isoformat()
                
                # Count results
                task_count = len(analysis_result.get('tasks', []))
                action_count = len(analysis_result.get('recommended_actions', []))
                
                print(f"  [OK] Analysis complete: {task_count} tasks, {action_count} actions")
                
                return analysis_result
                
            except json.JSONDecodeError as e:
                print(f"  [ERROR] Failed to parse AI response as JSON: {e}")
                return {
                    'conversation_file': conv_filename,
                    'conversation_id': conversation_data.get('conversation_id', ''),
                    'error': f"JSON parsing error: {str(e)}",
                    'raw_response': analysis_text if 'analysis_text' in locals() else None
                }
                
            except Exception as e:
                error_msg = str(e)
                if attempt < max_retries - 1 and "Connection error" in error_msg:
                    print(f"  [RETRY] Connection error on attempt {attempt + 1}/{max_retries}, retrying in {retry_delay}s...")
                    import time
                    time.sleep(retry_delay)
                    retry_delay *= 2  # Exponential backoff
                    continue
                else:
                    print(f"  [ERROR] Analysis failed after {attempt + 1} attempts: {e}")
                    return {
                        'conversation_file': conv_filename,
                        'conversation_id': conversation_data.get('conversation_id', ''),
                        'error': str(e)
                    }
    
    def process_conversation_file(self, conv_file_path):
        """
        Process a single conversation JSON file.
        
        Args:
            conv_file_path: Path to conversation JSON file
            
        Returns:
            Analysis result dictionary
        """
        try:
            with open(conv_file_path, 'r', encoding='utf-8') as f:
                conversation_data = json.load(f)
            
            # Check for skips
            chat_name = conversation_data.get('chat_name', '')
            conversation_id = conversation_data.get('conversation_id', '')
            
            # Skip by ID
            if conversation_id in self.skip_ids:
                reason = "ID in skip list"
                print(f"[SKIP] Skipping conversation {conversation_id} ({reason})")
                self.skipped_conversations.append({
                    'file': conv_file_path.name,
                    'conversation_id': conversation_id,
                    'chat_name': chat_name,
                    'reason': reason
                })
                return None

            # Skip by Name (contains)
            chat_name_lower = chat_name.lower()
            for skip_name in self.skip_names:
                if skip_name.lower() in chat_name_lower:
                    reason = f"Matches skip name '{skip_name}'"
                    print(f"[SKIP] Skipping conversation '{chat_name}' ({reason})")
                    self.skipped_conversations.append({
                        'file': conv_file_path.name,
                        'conversation_id': conversation_id,
                        'chat_name': chat_name,
                        'reason': reason
                    })
                    return None

            result = self.analyze_conversation(conversation_data, conv_file_path.name)
            self.all_results.append(result)
            
            return result
            
        except Exception as e:
            print(f"[ERROR] Failed to process {conv_file_path.name}: {e}")
            return {
                'conversation_file': conv_file_path.name,
                'error': str(e)
            }
    
    def process_all_conversations(self, conversations_dir, max_conversations=None):
        """
        Process all conversation JSON files in a directory.
        
        Args:
            conversations_dir: Path to directory containing conversation JSON files
            max_conversations: Maximum number of conversations to process (None for all)
        """
        conv_dir = Path(conversations_dir)
        
        if not conv_dir.exists():
            print(f"[ERROR] Conversations directory not found: {conv_dir}")
            return
        
        # Find all conversation JSON files
        conv_files = sorted(conv_dir.glob("conversation_*.json"))
        
        if not conv_files:
            print(f"[ERROR] No conversation JSON files found in {conv_dir}")
            return
        
        print(f"\n[INFO] Found {len(conv_files)} conversation files to process")
        if max_conversations:
            print(f"[INFO] Limit set to {max_conversations} conversations")
        print("=" * 80)
        
        # Process each conversation
        count = 0
        for idx, conv_file in enumerate(conv_files, 1):
            if max_conversations and count >= max_conversations:
                print(f"\n[STOP] Reached limit of {max_conversations} conversations.")
                break
                
            print(f"\n[{idx}/{len(conv_files)}] Processing: {conv_file.name}")
            result = self.process_conversation_file(conv_file)
            
            # Only increment count if we actually processed it (didn't skip)
            if result:
                count += 1
        
        print("\n" + "=" * 80)
        print(f"[COMPLETE] Processed {count} conversations")
    
    def save_results(self):
        """Save accumulated analysis results to files."""
        
        # Generate timestamp for filenames
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Sanitize target user email for filename
        user_id = self.target_user_email.split('@')[0]
        
        # Save complete results
        complete_file = self.output_dir / f"teams_analysis_complete_{user_id}_{timestamp}.json"
        try:
            with open(complete_file, 'w', encoding='utf-8') as f:
                json.dump({
                    'generated_at': datetime.now().isoformat(),
                    'target_user': self.target_user_email,
                    'total_conversations': len(self.all_results),
                    'conversations': self.all_results
                }, f, indent=2, ensure_ascii=False)
            
            print(f"\n[OK] Complete analysis saved: {complete_file}")
        except Exception as e:
            print(f"[ERROR] Failed to save complete results: {e}")
        
        # Generate summary with all conversations
        # Merge existing and new results
        summary_map = {item.get('conversation_id'): item for item in self.existing_summary}
        
        summary_results = []
        total_tasks = 0
        total_actions = 0
        total_todos = 0
        conversations_with_items = 0
        
        # Update map with new results
        for result in self.all_results:
            tasks = result.get('tasks', [])
            actions = result.get('recommended_actions', [])
            todos = []
            if isinstance(tasks, list):
                todos.extend(tasks)
            if isinstance(actions, list):
                todos.extend(actions)
            
            if tasks or actions:
                conversations_with_items += 1
                total_tasks += len(tasks)
                total_actions += len(actions)
                total_todos += len(todos)

            summary_item = {
                'conversation_file': result.get('conversation_file', ''),
                'conversation_id': result.get('conversation_id', ''),
                'chat_name': result.get('chat_name', ''),
                'summary': result.get('conversation_summary', {}),
                # Unified view: tasks + recommended_actions treated as a single todo list.
                # Keep legacy fields for backward compatibility with linking and older tooling.
                'todos': todos,
                'tasks': tasks,
                'recommended_actions': actions,
                'last_updated': datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
            }
            
            cid = result.get('conversation_id')
            if cid:
                summary_map[cid] = summary_item
            else:
                # Fallback for items without ID (unlikely)
                summary_results.append(summary_item)
        
        # Convert map back to list
        summary_results.extend(summary_map.values())
        
        # Save summary
        summary_file = self.output_dir / f"teams_analysis_summary_{user_id}_{timestamp}.json"
        try:
            with open(summary_file, 'w', encoding='utf-8') as f:
                json.dump({
                    'generated_at': datetime.now().isoformat(),
                    'target_user': self.target_user_email,
                    'total_conversations_analyzed': len(self.all_results),
                    'conversations_with_tasks_or_actions': conversations_with_items,
                    'total_todos_extracted': total_todos,
                    'total_tasks_extracted': total_tasks,
                    'total_recommended_actions': total_actions,
                    'results': summary_results
                }, f, indent=2, ensure_ascii=False)
            
            print(f"[OK] Summary saved: {summary_file}")
            print(f"[STATS] {total_todos} todos extracted (from {total_tasks} tasks + {total_actions} recommended actions)")
            
            # Create a copy with fixed name
            fixed_name_file = self.output_dir / f"teams_analysis_summary_{user_id}.json"
            shutil.copy2(summary_file, fixed_name_file)
            print(f"[OK] Summary copy created: {fixed_name_file}")
            
        except Exception as e:
            print(f"[ERROR] Failed to save summary: {e}")
            
        # Save skipped conversations log
        skipped_file = None
        if self.skipped_conversations:
            skipped_file = self.output_dir / f"teams_analysis_skipped_{user_id}_{timestamp}.json"
            try:
                with open(skipped_file, 'w', encoding='utf-8') as f:
                    json.dump({
                        'generated_at': datetime.now().isoformat(),
                        'target_user': self.target_user_email,
                        'total_skipped': len(self.skipped_conversations),
                        'skipped_conversations': self.skipped_conversations
                    }, f, indent=2, ensure_ascii=False)
                print(f"[OK] Skipped conversations log saved: {skipped_file}")
            except Exception as e:
                print(f"[ERROR] Failed to save skipped conversations log: {e}")
        
        return complete_file, summary_file, skipped_file


import argparse

def main():
    """Main function."""
    parser = argparse.ArgumentParser(description="Analyze Teams conversations using Azure OpenAI.")
    
    parser.add_argument("conversations_dir", type=Path, help="Directory containing conversation JSON files")
    parser.add_argument("--user", required=True, help="Email of target user to analyze for")
    parser.add_argument("--guide", type=Path, default=Path("Teams_Chat.md"), help="Path to the Teams_Chat.md guide")
    parser.add_argument("--output", type=Path, default=Path("output/teams_analysis"), help="Directory to save analysis results")
    parser.add_argument("--config", type=Path, default=Path(__file__).parent / "config.json", help="Path to config.json")
    parser.add_argument("--profile", type=Path, help="Path to user_profile.json")
    parser.add_argument("--recent-focus", type=Path, help="Optional: recent_focus.json path to derive active projects")
    parser.add_argument("--max", type=int, help="Maximum number of conversations to analyze")
    parser.add_argument("--existing-summary", type=Path, help="Path to existing summary JSON file to merge with")
    
    args = parser.parse_args()
    
    conversations_dir = args.conversations_dir
    guide_path = args.guide
    target_user_email = args.user
    output_dir = args.output
    config_path = args.config
    profile_path = args.profile
    recent_focus_path = args.recent_focus
    max_conversations = args.max
    existing_summary_path = args.existing_summary
    
    # Load user profile if provided
    user_profile = {}
    if profile_path and profile_path.exists():
        try:
            with open(profile_path, 'r', encoding='utf-8') as f:
                user_profile = json.load(f)
            print(f"[INFO] Loaded user profile from {profile_path}")
        except Exception as e:
            print(f"[WARNING] Failed to load user profile: {e}")

    # Validate inputs
    if not conversations_dir.exists():
        print(f"[ERROR] Conversations directory not found: {conversations_dir}")
        sys.exit(1)
    
    if not guide_path.exists():
        print(f"[ERROR] Guide file not found: {guide_path}")
        sys.exit(1)
    
    print("=" * 80)
    print("TEAMS CONVERSATION ANALYZER")
    print("=" * 80)
    print(f"Conversations: {conversations_dir}")
    print(f"Guide: {guide_path}")
    print(f"Target User: {target_user_email}")
    print(f"Output: {output_dir}")
    print(f"Config: {config_path}")
    if max_conversations:
        print(f"Max Conversations: {max_conversations}")
    print("=" * 80)
    
    rf_path = recent_focus_path.resolve() if recent_focus_path and recent_focus_path.exists() else None

    # Initialize analyzer
    analyzer = TeamsConversationAnalyzer(guide_path, target_user_email, output_dir, config_path, user_profile, existing_summary_path, rf_path)
    
    # Process all conversations
    analyzer.process_all_conversations(conversations_dir, max_conversations)
    
    # Save results
    complete_file, summary_file, skipped_file = analyzer.save_results()
    
    print("\n" + "=" * 80)
    print("[SUCCESS] Analysis complete!")
    print(f"[INFO] Complete results: {complete_file}")
    print(f"[INFO] Summary: {summary_file}")
    if skipped_file:
        print(f"[INFO] Skipped log: {skipped_file}")
    print("=" * 80)


if __name__ == "__main__":
    main()
