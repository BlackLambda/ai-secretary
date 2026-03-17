import argparse
import json
import sys
from pathlib import Path
from typing import Any, Iterable

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from ai_secretary_core.paths import RepoPaths


BASE_DIR = Path(__file__).resolve().parent.parent
PATHS = RepoPaths(BASE_DIR)


def _load_json(path: Path) -> Any:
    with path.open('r', encoding='utf-8') as f:
        return json.load(f)


def _dump_json(path: Path, data: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open('w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
        f.write('\n')


def _norm(s: str) -> str:
    return (s or '').strip().casefold()


def _clean_list(items: Any) -> list[str]:
    if not isinstance(items, list):
        return []
    out: list[str] = []
    seen: set[str] = set()
    for it in items:
        if not isinstance(it, str):
            continue
        cleaned = it.strip()
        if not cleaned:
            continue
        k = _norm(cleaned)
        if k in seen:
            continue
        seen.add(k)
        out.append(cleaned)
    return out


def _ordered_unique(primary: Iterable[str], extra: Iterable[str]) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for src in (primary, extra):
        for it in src:
            cleaned = str(it).strip()
            if not cleaned:
                continue
            k = _norm(cleaned)
            if k in seen:
                continue
            seen.add(k)
            out.append(cleaned)
    return out


def merge_topics(base_topics: dict, user_topics: dict) -> tuple[list[str], list[str]]:
    """Return (following, not_following).

    Rules:
    - Start from base lists in topics.json
    - Apply user_topics overrides (user wins):
      - any topic in user_topics.following => following
      - any topic in user_topics.not_following => not_following
    - Remove duplicates case-insensitively
    - Ensure a topic cannot be in both lists (not_following wins if conflict)
    """

    base_following = _clean_list(base_topics.get('following'))
    base_not = _clean_list(base_topics.get('not_following'))

    user_following = _clean_list(user_topics.get('following'))
    user_not = _clean_list(user_topics.get('not_following'))

    # Build candidates keeping stable order: base first, then user additions.
    following = _ordered_unique(base_following, user_following)
    not_following = _ordered_unique(base_not, user_not)

    user_not_keys = {_norm(x) for x in user_not}
    # Apply overrides: if user explicitly not_following, remove from following.
    following = [x for x in following if _norm(x) not in user_not_keys]

    user_following_keys = {_norm(x) for x in user_following}
    # If user explicitly following, remove from not_following.
    not_following = [x for x in not_following if _norm(x) not in user_following_keys]

    # Conflicts (should be rare): ensure disjoint; prefer not_following.
    not_keys = {_norm(x) for x in not_following}
    following = [x for x in following if _norm(x) not in not_keys]

    return following, not_following


def ensure_user_topics(path: Path) -> dict:
    if not path.exists():
        data = {'following': [], 'not_following': []}
        _dump_json(path, data)
        return data

    try:
        data = _load_json(path)
    except Exception:
        data = {}

    if not isinstance(data, dict):
        data = {}

    if 'following' not in data:
        data['following'] = []
    if 'not_following' not in data:
        data['not_following'] = []

    # Normalize/dedup in-place.
    data['following'] = _clean_list(data.get('following'))
    data['not_following'] = _clean_list(data.get('not_following'))
    _dump_json(path, data)
    return data


def sync_to_profile(
    *,
    topics_path: Path,
    user_topics_path: Path,
    profile_path: Path,
) -> dict:
    base_topics = _load_json(topics_path) if topics_path.exists() else {}
    if not isinstance(base_topics, dict):
        base_topics = {}

    user_topics = ensure_user_topics(user_topics_path)

    following, not_following = merge_topics(base_topics, user_topics)

    profile: dict = {}
    if profile_path.exists():
        try:
            profile_raw = _load_json(profile_path)
            if isinstance(profile_raw, dict):
                profile = profile_raw
        except Exception:
            profile = {}

    profile['following'] = following
    # Enforce the new shape: user_profile only has following list (no WATCH_ITEMS).
    if 'WATCH_ITEMS' in profile:
        profile.pop('WATCH_ITEMS', None)

    _dump_json(profile_path, profile)

    return {
        'following': following,
        'not_following': not_following,
        'profile': profile,
        'user_topics': user_topics,
        'topics': base_topics,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description='Sync topics.json + user_topics.json into user_profile.json.following')
    parser.add_argument('--topics', default=str(PATHS.topics_file()), help='Path to topics.json')
    parser.add_argument('--user-topics', default=str(PATHS.user_topics_file()), help='Path to user_state/user_topics.json')
    parser.add_argument('--profile', default=str(PATHS.user_profile_file()), help='Path to user_profile.json')
    parser.add_argument('--dry-run', action='store_true', help='Do not write files, print merged following/not_following')

    args = parser.parse_args()

    topics_path = Path(args.topics)
    user_topics_path = Path(args.user_topics)
    profile_path = Path(args.profile)

    base_topics = _load_json(topics_path) if topics_path.exists() else {}
    if not isinstance(base_topics, dict):
        base_topics = {}

    if args.dry_run:
        user_topics = ensure_user_topics(user_topics_path)
        following, not_following = merge_topics(base_topics, user_topics)
        print(json.dumps({'following': following, 'not_following': not_following}, ensure_ascii=False, indent=2))
        return 0

    sync_to_profile(topics_path=topics_path, user_topics_path=user_topics_path, profile_path=profile_path)
    print(f"Synced topics into {profile_path} (following={len(_load_json(profile_path).get('following', []))}).")
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
