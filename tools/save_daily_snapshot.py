#!/usr/bin/env python3
"""
Tool: Save Daily Snapshot
Description: Saves daily snapshots of option flow data with timestamps for historical tracking
"""

import os
import json
import shutil
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

def save_snapshot():
    """Save today's data as a dated snapshot"""
    today = datetime.now().strftime('%Y-%m-%d')
    snapshot_dir = '.tmp/snapshots'

    # Create snapshots directory if it doesn't exist
    os.makedirs(snapshot_dir, exist_ok=True)

    # Files to snapshot
    files_to_save = [
        ('.tmp/option_flow_data.csv', f'{snapshot_dir}/flow_{today}.csv'),
        ('.tmp/raw_sheet_data.json', f'{snapshot_dir}/raw_{today}.json')
    ]

    saved_files = []
    for source, dest in files_to_save:
        if os.path.exists(source):
            shutil.copy2(source, dest)
            size = os.path.getsize(dest) / (1024 * 1024)  # MB
            saved_files.append(f"{dest} ({size:.1f}MB)")
            print(f"✓ Saved: {dest}")

    # Create metadata
    metadata = {
        'date': today,
        'timestamp': datetime.now().isoformat(),
        'files': saved_files
    }

    with open(f'{snapshot_dir}/metadata_{today}.json', 'w') as f:
        json.dump(metadata, f, indent=2)

    print(f"\n✓ Daily snapshot saved for {today}")
    return snapshot_dir

def list_snapshots():
    """List all available snapshots"""
    snapshot_dir = '.tmp/snapshots'
    if not os.path.exists(snapshot_dir):
        print("No snapshots found")
        return []

    snapshots = sorted([f for f in os.listdir(snapshot_dir) if f.startswith('flow_')])
    print(f"\nAvailable snapshots: {len(snapshots)}")
    for snap in snapshots:
        date = snap.replace('flow_', '').replace('.csv', '')
        size = os.path.getsize(f'{snapshot_dir}/{snap}') / 1024
        print(f"  {date}: {size:.0f}KB")

    return snapshots

def main():
    """Main execution"""
    print("=== Daily Snapshot Manager ===\n")
    save_snapshot()
    print()
    list_snapshots()

if __name__ == "__main__":
    main()
