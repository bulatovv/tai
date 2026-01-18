# /// script
# requires-python = ">=3.11"
# dependencies = [
#     "duckdb",
#     "trio",
#     "httpx",
#     "pydantic",
#     "pydantic-settings",
#     "tenacity",
# ]
# ///
import os
import sys
from collections import defaultdict

import duckdb

# Add src to path to import tai
sys.path.append(os.path.abspath('src'))

from tai.world_tagging import infer_tags


def format_metadata(metadata: dict) -> str:
    """Format metadata dictionary into a readable string."""
    if not metadata:
        return 'None'

    parts = []
    # Sort by tag name for stability
    for tag in sorted(metadata.keys()):
        info = metadata[tag]
        if 'match' in info:
            parts.append(f"{tag} (match: '{info['match']}')")
        elif 'inference' in info:
            parts.append(f'{tag} (inferred from: {info["inference"]})')
        else:
            parts.append(f'{tag} (unknown source)')

    return ', '.join(parts)


def main():
    """Generate a tagging report from the database."""
    db_path = 'data/worlds_online.db'
    if not os.path.exists(db_path):
        print(f'Database not found at {db_path}')
        return

    print('Connecting to database...')
    con = duckdb.connect(db_path)

    print('Fetching unique world names...')
    # Fetch distinct world names.
    # Note: 'name' column is in 'worlds_online' table.
    try:
        rows = con.execute('SELECT DISTINCT name FROM worlds_online').fetchall()
    except duckdb.CatalogException:
        print("Table 'worlds_online' not found. Checking 'world_sessions'...")
        # Fallback if worlds_online is empty or not initialized yet?
        # But per schema it should be there.
        try:
            rows = con.execute('SELECT DISTINCT name FROM world_sessions').fetchall()
        except duckdb.CatalogException:
            print('No relevant tables found.')
            return

    world_names = [row[0] for row in rows if row[0]]
    print(f'Found {len(world_names)} unique worlds.')

    grouped_worlds = defaultdict(list)

    for name in world_names:
        tags, metadata = infer_tags(name, return_metadata=True)
        meta_str = format_metadata(metadata)
        grouped_worlds[meta_str].append(name)

    # Sort groups: Put "None" at the end, others sorted alphabetically
    sorted_keys = sorted(grouped_worlds.keys())
    if 'None' in sorted_keys:
        sorted_keys.remove('None')
        sorted_keys.append('None')

    output_file = 'ad-hoc/tagging_report.txt'
    print(f'Writing report to {output_file}...')

    with open(output_file, 'w', encoding='utf-8') as f:
        for key in sorted_keys:
            worlds = sorted(grouped_worlds[key])
            f.write(f'# {key}\n')
            for w in worlds:
                f.write(f'- {w}\n')
            f.write('\n')

    print('Done.')


if __name__ == '__main__':
    main()
