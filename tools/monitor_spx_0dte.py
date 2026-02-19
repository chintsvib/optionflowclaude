#!/usr/bin/env python3
"""
Tool: Monitor SPX 0DTE Signal Cell
Description: Reads cell A4 from the "Advanced 0DTE" sheet and sends a Telegram
             alert when it contains "Buy Calls" or "Buy Puts".

Usage:
    python tools/monitor_spx_0dte.py              # default
    python tools/monitor_spx_0dte.py --dry-run    # print without sending
"""

import os
import sys
import json
import argparse
from datetime import datetime, date
from zoneinfo import ZoneInfo

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from tools.read_google_sheet import read_sheet
from tools.send_telegram import send_telegram

TMP_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), ".tmp")
STATE_FILE = os.path.join(TMP_DIR, "spx_0dte_signal_state.json")


def _load_state():
    """Load last-sent signal state."""
    try:
        with open(STATE_FILE, "r") as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {}


def _save_state(state):
    os.makedirs(TMP_DIR, exist_ok=True)
    with open(STATE_FILE, "w") as f:
        json.dump(state, f)


def check_signal(config, dry_run=False):
    """Read cell A4 and alert if it says Buy Calls or Buy Puts."""
    sheet_cfg = config["SPX_0DTE"]
    data = read_sheet(sheet_cfg["sheet_url"], sheet_cfg["sheet_name"], "A4:A4")

    if not data or not data[0]:
        print("  SPX 0DTE signal cell A4: empty")
        return

    value = str(data[0][0]).strip()
    print(f"  SPX 0DTE signal cell A4: '{value}'")

    # Strip trailing punctuation for matching (cell may have "Buy Puts?" with ?)
    value_clean = value.lower().rstrip("?! ")
    if value_clean not in ("buy calls", "buy puts"):
        print("  No actionable signal.")
        return

    # Dedup: only alert once per signal change per day
    state = _load_state()
    today = str(date.today())
    last_signal = state.get("last_signal", "")
    last_date = state.get("last_date", "")

    if last_signal == value_clean and last_date == today:
        print(f"  Already alerted '{value}' today. Skipping.")
        return

    if value_clean == "buy calls":
        signal_text = "Bullish Flow: Buy Calls?"
    else:
        signal_text = "Bearish Flow: Buy Puts?"

    message = f"<b>SPX 0DTE Alert</b>\n\n{signal_text}"

    print(f"  ALERT: {value}")

    if dry_run:
        print(f"  [DRY RUN] Would send: {message}")
    else:
        send_telegram(message)

    _save_state({"last_signal": value_clean, "last_date": today})


def main():
    parser = argparse.ArgumentParser(description="Monitor SPX 0DTE signal cell")
    parser.add_argument("--dry-run", action="store_true",
                        help="Print alerts without sending Telegram message")
    args = parser.parse_args()

    print("=" * 60)
    print(f"SPX 0DTE SIGNAL MONITOR - {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print("=" * 60)

    config_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "config.json")
    with open(config_path, "r") as f:
        config = json.load(f)

    if "SPX_0DTE" not in config:
        print("SPX_0DTE not found in config.json")
        return

    check_signal(config, dry_run=args.dry_run)
    print("\nDone.")


if __name__ == "__main__":
    main()
