"""
JSON Writer - Utility for saving data to JSON files.
"""

import json
import os
from typing import Any
from pathlib import Path


def save_json(data: Any, filename: str, output_dir: str = "output") -> str:
    """
    Save data to JSON file.

    Args:
        data: Python object to save (dict, list, etc.)
        filename: Output filename (e.g., "top_collaborators.json")
        output_dir: Output directory (default: "output")

    Returns:
        str: Full path to saved file

    Example:
        >>> save_json({"users": [1, 2, 3]}, "users.json")
        'output/users.json'
    """
    # Get absolute path to output directory
    if not os.path.isabs(output_dir):
        base_dir = Path(__file__).parent.parent.parent
        output_path = base_dir / output_dir
    else:
        output_path = Path(output_dir)

    # Create output directory if needed
    output_path.mkdir(parents=True, exist_ok=True)

    # Full file path
    file_path = output_path / filename

    # Write JSON with nice formatting
    with open(file_path, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=4, ensure_ascii=False, default=str)

    print(f"[SAVED] {file_path}")

    return str(file_path)
