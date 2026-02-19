#!/usr/bin/env python3
"""
Railway Entry Point: 7DTE Option Flow Monitor Loop

Runs monitor_7day_alerts every 5 minutes during US market hours
(Mon-Fri 9:30 AM - 4:00 PM Eastern), then sleeps until next market open.
"""

import os
import time
import sys
import json
import traceback
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

# Import monitors
from tools.monitor_7day_alerts import main as run_monitor
from tools.monitor_floor_alerts import main as run_floor_monitor

# Import SPX 0DTE signal monitor
from tools.monitor_spx_0dte import check_signal as check_spx_0dte_signal

# Import cross-reference engine
from tools.allday_db import init_db, load_allday_to_db, is_db_loaded_today
from tools.multi_source_check import main as run_multi_source_check

ET = ZoneInfo("America/New_York")
INTERVAL_SECONDS = 60  # 1 minute

# Market hours (Eastern Time)
MARKET_OPEN_HOUR, MARKET_OPEN_MIN = 9, 30
MARKET_CLOSE_HOUR, MARKET_CLOSE_MIN = 16, 0


def now_et():
    return datetime.now(ET)


def is_market_hours():
    """Check if current time is within US market hours (Mon-Fri 9:30-16:00 ET)."""
    t = now_et()
    # Monday=0, Friday=4
    if t.weekday() > 4:
        return False
    market_open = t.replace(hour=MARKET_OPEN_HOUR, minute=MARKET_OPEN_MIN, second=0, microsecond=0)
    market_close = t.replace(hour=MARKET_CLOSE_HOUR, minute=MARKET_CLOSE_MIN, second=0, microsecond=0)
    return market_open <= t <= market_close


def seconds_until_next_market_open():
    """Calculate seconds until the next market open."""
    t = now_et()
    today_open = t.replace(hour=MARKET_OPEN_HOUR, minute=MARKET_OPEN_MIN, second=0, microsecond=0)

    if t.weekday() <= 4 and t < today_open:
        # Today is a weekday and market hasn't opened yet
        return (today_open - t).total_seconds()

    # Find next weekday
    days_ahead = 1
    next_day = t + timedelta(days=days_ahead)
    while next_day.weekday() > 4:
        days_ahead += 1
        next_day = t + timedelta(days=days_ahead)

    next_open = next_day.replace(hour=MARKET_OPEN_HOUR, minute=MARKET_OPEN_MIN, second=0, microsecond=0)
    return (next_open - t).total_seconds()


def main():
    print("=" * 60)
    print("OPTION FLOW MONITOR - LOOP MODE")
    print(f"Interval: {INTERVAL_SECONDS // 60} minutes")
    print(f"Market hours: {MARKET_OPEN_HOUR}:{MARKET_OPEN_MIN:02d} - {MARKET_CLOSE_HOUR}:{MARKET_CLOSE_MIN:02d} ET")
    print("=" * 60)

    # Patch sys.argv so the monitor's argparse doesn't see loop args
    sys.argv = [sys.argv[0]]

    # Initialize allDay SQLite database
    print("\nInitializing allDay database...")
    init_db()

    # First run after deploy: seed dedup state without sending alerts
    # (Railway's ephemeral filesystem wipes state files on each deploy)
    first_run = True

    while True:
        if is_market_hours():
            if first_run:
                print(f"\n[{now_et().strftime('%Y-%m-%d %H:%M:%S ET')}] First run — seeding dedup state (no alerts)...")
                sys.argv = [sys.argv[0], "--dry-run"]
            else:
                print(f"\n[{now_et().strftime('%Y-%m-%d %H:%M:%S ET')}] Running monitors...")

            # Load allDay data once per day
            if not is_db_loaded_today():
                try:
                    print("Loading allDay data into SQLite (once per day)...")
                    load_allday_to_db()
                except Exception as e:
                    print(f"allDay DB load error: {e}")
                    traceback.print_exc()

            # 7DTE monitor
            try:
                run_monitor()
            except Exception as e:
                print(f"7DTE monitor error (will retry next interval): {e}")
                traceback.print_exc()

            # Floor Trader monitor
            try:
                run_floor_monitor()
            except Exception as e:
                print(f"Floor monitor error (will retry next interval): {e}")
                traceback.print_exc()

            # SPX 0DTE signal monitor
            try:
                cfg_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "config.json")
                with open(cfg_path, "r") as f:
                    cfg = json.load(f)
                check_spx_0dte_signal(cfg, dry_run=first_run)
            except Exception as e:
                print(f"SPX 0DTE monitor error (will retry next interval): {e}")
                traceback.print_exc()

            # Multi-source confirmation — DISABLED for analysis tuning
            # try:
            #     run_multi_source_check()
            # except Exception as e:
            #     print(f"Multi-source check error (will retry next interval): {e}")
            #     traceback.print_exc()

            if first_run:
                sys.argv = [sys.argv[0]]
                first_run = False
                print("Dedup state seeded. Next run will send real alerts.")

            print(f"Sleeping {INTERVAL_SECONDS // 60} minutes...")
            time.sleep(INTERVAL_SECONDS)
        else:
            wait = seconds_until_next_market_open()
            wait_hours = wait / 3600
            next_open = now_et() + timedelta(seconds=wait)
            print(f"\n[{now_et().strftime('%Y-%m-%d %H:%M:%S ET')}] Market closed.")
            print(f"Next open: {next_open.strftime('%A %Y-%m-%d %H:%M ET')} ({wait_hours:.1f} hours)")
            # Sleep in chunks so we can log periodically
            while wait > 0:
                chunk = min(wait, 3600)  # sleep max 1 hour at a time
                time.sleep(chunk)
                wait -= chunk


if __name__ == "__main__":
    main()
