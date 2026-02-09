#!/usr/bin/env python3
"""
Daily Option Flow Analysis Pipeline
Run this script daily to:
1. Fetch latest data from Google Sheets
2. Process with strike-level detail
3. Identify repeated flows
4. Create interactive dashboard
5. Save daily snapshot
"""

import sys
import subprocess
import argparse
from datetime import datetime

def run_tool(script_path, description, extra_args=None):
    """Run a tool script and handle errors"""
    print(f"\n{'='*70}")
    print(f"STEP: {description}")
    print(f"{'='*70}\n")

    try:
        cmd = [sys.executable, script_path]
        if extra_args:
            cmd.extend(extra_args)

        result = subprocess.run(
            cmd,
            check=True,
            capture_output=False
        )
        return True
    except subprocess.CalledProcessError as e:
        print(f"\n❌ Error running {script_path}")
        print(f"Exit code: {e.returncode}")
        return False
    except Exception as e:
        print(f"\n❌ Unexpected error: {e}")
        return False

def main():
    """Run the complete daily analysis pipeline"""
    # Parse command-line arguments
    parser = argparse.ArgumentParser(description='Daily Option Flow Analysis')
    parser.add_argument('--past', type=int, default=15,
                       help='Number of business days back to analyze, excluding weekends (default: 15)')
    args = parser.parse_args()

    days_back = args.past

    print("\n" + "="*70)
    print(f"DAILY OPTION FLOW ANALYSIS - {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print(f"Analyzing past {days_back} business days")
    print("="*70)

    # Define pipeline steps
    steps = [
        ("tools/read_google_sheet.py", "Step 1/6: Fetching data from Google Sheets", None),
        ("tools/process_detailed_flow.py", "Step 2/6: Processing with strike-level detail", ['--days', str(days_back)]),
        ("tools/fetch_ema_status.py", "Step 3/6: Fetching EMA status across timeframes", None),
        ("tools/create_interactive_dashboard.py", "Step 4/6: Creating interactive dashboard", ['--days', str(days_back)]),
        ("tools/save_daily_snapshot.py", "Step 5/6: Saving daily snapshot", None),
    ]

    # Execute each step
    for script, description, extra_args in steps:
        success = run_tool(script, description, extra_args)
        if not success:
            print(f"\n❌ Pipeline failed at: {description}")
            print("Please fix the error and try again.")
            sys.exit(1)

    # Success!
    print("\n" + "="*70)
    print("✅ DAILY ANALYSIS COMPLETED SUCCESSFULLY!")
    print("="*70)

    # Find the most recent dashboard file
    import glob
    import os
    dashboard_files = glob.glob(f'.tmp/option_flow_dashboard_past{days_back}_*.html')
    if dashboard_files:
        # Get the most recently created file
        latest_dashboard = max(dashboard_files, key=os.path.getctime)
        dashboard_filename = os.path.basename(latest_dashboard)
    else:
        dashboard_filename = f"option_flow_dashboard_past{days_back}_*.html"

    print("\n📊 Outputs:")
    print(f"  📈 .tmp/{dashboard_filename} - Interactive dashboard (OPEN THIS!)")
    print("  📄 .tmp/detailed_flow.csv - Strike-level data")
    print("  🔁 .tmp/repeated_flows.csv - Repeated flow patterns")
    print("  💾 .tmp/snapshots/ - Historical snapshots")
    print("  📊 .tmp/ema_status.csv - EMA status across timeframes")
    print("\n💡 Tip: Open the HTML dashboard in your browser for full interactive analysis!")

if __name__ == "__main__":
    main()
