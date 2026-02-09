#!/usr/bin/env python3
"""
Master script to run the complete option flow analysis pipeline
"""

import sys
import subprocess

def run_tool(script_path, description):
    """Run a tool script and handle errors"""
    print(f"\n{'='*60}")
    print(f"STEP: {description}")
    print(f"{'='*60}\n")

    try:
        result = subprocess.run(
            [sys.executable, script_path],
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
    """Run the complete pipeline"""
    print("\n" + "="*60)
    print("OPTION FLOW ANALYSIS PIPELINE")
    print("="*60)

    # Define pipeline steps
    steps = [
        ("tools/read_google_sheet.py", "Reading data from Google Sheets"),
        ("tools/process_option_data.py", "Processing and aggregating option data"),
        ("tools/visualize_option_flow.py", "Generating visualizations")
    ]

    # Execute each step
    for script, description in steps:
        success = run_tool(script, description)
        if not success:
            print(f"\n❌ Pipeline failed at: {description}")
            print("Please fix the error and try again.")
            sys.exit(1)

    # Success!
    print("\n" + "="*60)
    print("✓ PIPELINE COMPLETED SUCCESSFULLY!")
    print("="*60)
    print("\nOutputs:")
    print("  📊 .tmp/dollar_flow_chart.png - Dollar flow by ticker")
    print("  📊 .tmp/call_put_qty_chart.png - Call vs Put quantities")
    print("  📄 .tmp/option_flow_data.csv - Processed data")
    print("\nOpen the charts to view your option flow analysis!")

if __name__ == "__main__":
    main()
