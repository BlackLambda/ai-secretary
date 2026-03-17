"""
Get top N collaborators based on email frequency.

This script:
1. Identifies the most frequent email contacts
2. Saves the results to output/top_collaborators.json
3. Allows users to verify the accuracy and recall of collaborators

The output file is then used by:
- get_collaborator_exchanges.py (to extract email conversations)
- get_collaborator_teams_threads.py (to extract Teams conversations)

This ensures both scripts use the same collaborator list.
"""

import sys
import os

# Add current directory to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from src.services.collaborators import CollaboratorsService
from src.utils.json_writer import save_json


def main():
    import argparse

    parser = argparse.ArgumentParser(
        description="Get top N collaborators based on email frequency"
    )
    parser.add_argument(
        '--top',
        type=int,
        default=10,
        help='Number of top collaborators to extract (default: 10)'
    )

    args = parser.parse_args()

    print(f"\n{'='*60}")
    print(f"Extracting Top {args.top} Collaborators")
    print(f"{'='*60}")

    # Get top collaborators
    collab_service = CollaboratorsService()
    result = collab_service.get_top_collaborators(top_n=args.top)

    # Save results
    save_json(result, "top_collaborators.json")

    # Print summary
    print(f"\n{'='*60}")
    print(f"Summary")
    print(f"{'='*60}")
    print(f"Total collaborators found: {result['count']}")
    print(f"\nTop {result['count']} Collaborators:")
    for i, collab in enumerate(result['collaborators'], 1):
        print(f"  {i}. {collab['alias']} ({collab['email']})")

    print(f"\n[SUCCESS] Results saved to output/top_collaborators.json")
    print(f"[NEXT STEP] Review the collaborators list, then run:")
    print(f"  - python get_collaborator_exchanges.py  (for email exchanges)")
    print(f"  - python get_collaborator_teams_threads.py  (for Teams threads)")


if __name__ == "__main__":
    main()
