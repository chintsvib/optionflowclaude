#!/usr/bin/env python3
"""
Tool: Monitor 7Day Sheet for Large Flows
Description: Reads the 7Day (<7DTE) option flow sheet, checks for large
             dollar amounts or quantities, and sends Telegram alerts.

Thresholds (configurable via CLI args):
  - Call$ or Put$ > $1,000,000
  - Call Qty or Put Qty > 1,000

Usage:
    python tools/monitor_7day_alerts.py                  # defaults
    python tools/monitor_7day_alerts.py --dollar 2000000 # custom dollar threshold
    python tools/monitor_7day_alerts.py --qty 500        # custom qty threshold
    python tools/monitor_7day_alerts.py --dry-run        # print alerts without sending
"""

import os
import sys
import json
import re
import hashlib
import argparse
from datetime import datetime, date
from zoneinfo import ZoneInfo

# Add project root to path so we can import sibling tools
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from tools.read_google_sheet import get_credentials, extract_sheet_id, read_sheet
from tools.send_telegram import send_telegram


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def parse_dollar(value):
    """
    Parse a dollar string into a float.
    Handles: "$1,234,567", "$1.2M", "1234567", "$500K", empty/missing, etc.
    Returns 0.0 if unparseable.
    """
    if not value or not str(value).strip():
        return 0.0
    s = str(value).strip().replace("$", "").replace(",", "").strip()
    try:
        # Handle suffixes like M, K, B
        multiplier = 1.0
        if s[-1].upper() == "M":
            multiplier = 1_000_000
            s = s[:-1]
        elif s[-1].upper() == "K":
            multiplier = 1_000
            s = s[:-1]
        elif s[-1].upper() == "B":
            multiplier = 1_000_000_000
            s = s[:-1]
        return float(s) * multiplier
    except (ValueError, IndexError):
        return 0.0


def parse_qty(value):
    """
    Parse a quantity string into a float.
    Handles: "1,234", "1234", "1.2K", empty/missing, etc.
    Returns 0.0 if unparseable.
    """
    if not value or not str(value).strip():
        return 0.0
    s = str(value).strip().replace(",", "").strip()
    try:
        multiplier = 1.0
        if s[-1].upper() == "K":
            multiplier = 1_000
            s = s[:-1]
        elif s[-1].upper() == "M":
            multiplier = 1_000_000
            s = s[:-1]
        return float(s) * multiplier
    except (ValueError, IndexError):
        return 0.0


def find_column_index(headers, *patterns):
    """
    Find the column index whose header matches any of the given patterns
    (case-insensitive substring match, whitespace-normalized).
    Returns index or None.
    """
    for i, h in enumerate(headers):
        # Normalize: replace newlines/tabs with space, collapse multiple spaces
        h_norm = re.sub(r"\s+", " ", str(h)).strip().lower()
        for pat in patterns:
            if pat.lower() in h_norm:
                return i
    return None


def safe_get(row, idx, default=""):
    """Safely get a value from a row list by index."""
    if idx is not None and idx < len(row):
        return row[idx]
    return default


# ---------------------------------------------------------------------------
# Deduplication — only alert on NEW entries since last run
# ---------------------------------------------------------------------------

STATE_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), ".tmp")
STATE_FILE = os.path.join(STATE_DIR, "7day_alert_state.json")


def _alert_key(alert):
    """Create a unique key for an alert based on its content."""
    raw = f"{alert['side']}|{alert['label']}|{alert['field']}|{alert['value']}"
    return hashlib.md5(raw.encode()).hexdigest()


def load_state():
    """Load previously seen alert keys. Resets automatically each day."""
    if not os.path.exists(STATE_FILE):
        return {"date": str(date.today()), "seen": []}
    try:
        with open(STATE_FILE, "r") as f:
            state = json.load(f)
        # Reset if it's a new day (sheet data is daily)
        if state.get("date") != str(date.today()):
            return {"date": str(date.today()), "seen": []}
        return state
    except (json.JSONDecodeError, KeyError):
        return {"date": str(date.today()), "seen": []}


def save_state(state):
    """Persist seen alert keys to disk."""
    os.makedirs(STATE_DIR, exist_ok=True)
    with open(STATE_FILE, "w") as f:
        json.dump(state, f, indent=2)


def filter_new_alerts(alerts):
    """Return only alerts we haven't sent yet today, and update state."""
    state = load_state()
    seen = set(state["seen"])
    new_alerts = []
    for a in alerts:
        key = _alert_key(a)
        if key not in seen:
            new_alerts.append(a)
            seen.add(key)
    state["seen"] = list(seen)
    save_state(state)
    return new_alerts


# ---------------------------------------------------------------------------
# Core logic
# ---------------------------------------------------------------------------

def check_side(side_label, headers, data_rows, dollar_threshold, qty_threshold):
    """
    Check one side (buying or selling) for threshold breaches.

    Returns a list of alert dicts:
      { side, row_num, ticker, expiry, field, value, threshold }
    """
    alerts = []

    # Try to find relevant columns by common header names
    date_idx = find_column_index(headers, "today's date", "date")
    time_idx = find_column_index(headers, "time")
    ticker_idx = find_column_index(headers, "ticker", "symbol", "stock")
    xmonth_idx = find_column_index(headers, "xmonth")
    xdate_idx = find_column_index(headers, "xdate")
    expiry_idx = find_column_index(headers, "expiry", "exp", "expiration")
    strike_idx = find_column_index(headers, "strike")
    call_dollar_idx = find_column_index(headers, "calls $", "call$", "call $", "call dollar")
    put_dollar_idx = find_column_index(headers, "puts $", "put$", "put $", "put dollar")
    call_qty_idx = find_column_index(headers, "calls qty", "call qty", "call quantity", "# calls", "call vol")
    put_qty_idx = find_column_index(headers, "puts qty", "put qty", "put quantity", "# puts", "put vol")
    insights_idx = find_column_index(headers, "order insights", "insights")

    print(f"\n  [{side_label}] Column mapping:")
    print(f"    Ticker:   col {ticker_idx}  ({safe_get(headers, ticker_idx, '?')})")
    print(f"    Expiry:   col {expiry_idx} / xMonth={xmonth_idx} xDate={xdate_idx}")
    print(f"    Strike:   col {strike_idx}  ({safe_get(headers, strike_idx, '?')})")
    print(f"    Call$:    col {call_dollar_idx}  ({safe_get(headers, call_dollar_idx, '?')})")
    print(f"    Put$:     col {put_dollar_idx}  ({safe_get(headers, put_dollar_idx, '?')})")
    print(f"    Call Qty: col {call_qty_idx}  ({safe_get(headers, call_qty_idx, '?')})")
    print(f"    Put Qty:  col {put_qty_idx}  ({safe_get(headers, put_qty_idx, '?')})")

    for row_offset, row in enumerate(data_rows):
        # Sheet row number (1-indexed, accounting for header at row 4, data starts row 5)
        sheet_row = row_offset + 5
        row_date = safe_get(row, date_idx, "")
        row_time = safe_get(row, time_idx, "")
        ticker = safe_get(row, ticker_idx, "???")
        # Build expiry from xMonth+xDate if no single expiry column
        if expiry_idx is not None:
            expiry = safe_get(row, expiry_idx, "")
        elif xmonth_idx is not None and xdate_idx is not None:
            expiry = f"{safe_get(row, xmonth_idx, '')} {safe_get(row, xdate_idx, '')}".strip()
        else:
            expiry = ""
        strike = safe_get(row, strike_idx, "")
        label = f"{ticker} {expiry} {strike}".strip()
        insights = safe_get(row, insights_idx, "")

        # Common fields for each alert from this row
        base = {
            "side": side_label, "row": sheet_row, "label": label,
            "date": row_date, "time": row_time, "insights": insights,
        }

        # Check Call$
        if call_dollar_idx is not None:
            val = parse_dollar(safe_get(row, call_dollar_idx))
            if val > dollar_threshold:
                alerts.append({**base, "field": "Call$", "value": val, "threshold": dollar_threshold})

        # Check Put$
        if put_dollar_idx is not None:
            val = parse_dollar(safe_get(row, put_dollar_idx))
            if val > dollar_threshold:
                alerts.append({**base, "field": "Put$", "value": val, "threshold": dollar_threshold})

        # Check Call Qty
        if call_qty_idx is not None:
            val = parse_qty(safe_get(row, call_qty_idx))
            if val > qty_threshold:
                alerts.append({**base, "field": "Call Qty", "value": val, "threshold": qty_threshold})

        # Check Put Qty
        if put_qty_idx is not None:
            val = parse_qty(safe_get(row, put_qty_idx))
            if val > qty_threshold:
                alerts.append({**base, "field": "Put Qty", "value": val, "threshold": qty_threshold})

    return alerts


def format_number(val):
    """Format a number for display ($1.23M or 1,234)."""
    if val >= 1_000_000:
        return f"${val/1_000_000:,.2f}M"
    elif val >= 1_000:
        return f"${val/1_000:,.1f}K" if val > 10_000 else f"${val:,.0f}"
    return f"${val:,.0f}"


def format_qty(val):
    """Format quantity for display."""
    if val >= 1_000_000:
        return f"{val/1_000_000:,.2f}M"
    return f"{val:,.0f}"


def build_alert_message(alerts):
    """Build a Telegram-friendly HTML alert message."""
    now = datetime.now(ZoneInfo("America/New_York")).strftime("%Y-%m-%d %H:%M ET")
    lines = [f"<b>7DTE Option Flow Alert</b>  ({now})\n"]

    for a in alerts:
        if "Qty" in a["field"]:
            display_val = format_qty(a["value"])
            display_thresh = format_qty(a["threshold"])
        else:
            display_val = format_number(a["value"])
            display_thresh = format_number(a["threshold"])

        # Date/time from the sheet row
        when = f"{a.get('date', '')} {a.get('time', '')}".strip()
        when_line = f"  {when}\n" if when else ""

        # Order Insights
        insights = a.get("insights", "")
        insights_line = f"  Insight: <i>{insights}</i>\n" if insights else ""

        lines.append(
            f"{when_line}"
            f"  <b>{a['label']}</b> ({a['side']})\n"
            f"  {a['field']}: <b>{display_val}</b> (threshold: {display_thresh})\n"
            f"{insights_line}"
        )

    lines.append(f"Total alerts: {len(alerts)}")
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Monitor 7Day sheet for large option flows")
    parser.add_argument("--dollar", type=float, default=1_000_000,
                        help="Dollar threshold for Call$/Put$ (default: 1000000)")
    parser.add_argument("--qty", type=float, default=1_000,
                        help="Quantity threshold for Call Qty/Put Qty (default: 1000)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Print alerts without sending Telegram message")
    args = parser.parse_args()

    print("=" * 60)
    print(f"7DTE OPTION FLOW MONITOR - {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print(f"Thresholds: ${args.dollar:,.0f} (dollar)  |  {args.qty:,.0f} (qty)")
    print("=" * 60)

    # Load config
    config_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "config.json")
    with open(config_path, "r") as f:
        config = json.load(f)

    sheet_cfg = config["7Day"]

    # --- Read BUYING side (header + data) ---
    # Read from header_row to get headers, then data starts at header_row+1
    header_row = sheet_cfg["header_row"]  # 4
    buying_range_col = sheet_cfg["range_buying"].split(":")[1]  # "O"
    buying_start_col = sheet_cfg["range_buying"].split(":")[0].rstrip("0123456789")  # "A"
    selling_range_col = sheet_cfg["range_selling"].split(":")[1]  # "AE"
    selling_start_col = sheet_cfg["range_selling"].split(":")[0].rstrip("0123456789")  # "R"

    # Read buying: header + data
    buying_header_range = f"{buying_start_col}{header_row}:{buying_range_col}"
    print(f"\nReading BUYING side ({buying_header_range})...")
    buying_all = read_sheet(sheet_cfg["sheet_url"], sheet_cfg["sheet_name"], buying_header_range)

    if not buying_all or len(buying_all) < 2:
        print("  No buying data found (need at least header + 1 data row).")
        buying_headers, buying_data = [], []
    else:
        buying_headers = buying_all[0]
        buying_data = buying_all[1:]
        print(f"  Header: {buying_headers}")
        print(f"  Data rows: {len(buying_data)}")

    # Read selling: header + data
    selling_header_range = f"{selling_start_col}{header_row}:{selling_range_col}"
    print(f"\nReading SELLING side ({selling_header_range})...")
    selling_all = read_sheet(sheet_cfg["sheet_url"], sheet_cfg["sheet_name"], selling_header_range)

    if not selling_all or len(selling_all) < 2:
        print("  No selling data found (need at least header + 1 data row).")
        selling_headers, selling_data = [], []
    else:
        selling_headers = selling_all[0]
        selling_data = selling_all[1:]
        print(f"  Header: {selling_headers}")
        print(f"  Data rows: {len(selling_data)}")

    # --- Check thresholds ---
    print("\nChecking thresholds...")
    all_alerts = []

    if buying_headers:
        all_alerts.extend(
            check_side("BUYING", buying_headers, buying_data, args.dollar, args.qty)
        )

    if selling_headers:
        all_alerts.extend(
            check_side("SELLING", selling_headers, selling_data, args.dollar, args.qty)
        )

    # --- Report ---
    if not all_alerts:
        print("\nNo alerts triggered. All values within thresholds.")
        return

    print(f"\nTotal alerts matching thresholds: {len(all_alerts)}")

    # Deduplicate: only send NEW alerts (not seen in previous runs today)
    new_alerts = filter_new_alerts(all_alerts)

    if not new_alerts:
        print("No NEW alerts since last run. All already notified.")
        return

    print(f"\n{'='*60}")
    print(f"NEW ALERTS: {len(new_alerts)}  (of {len(all_alerts)} total)")
    print(f"{'='*60}")

    message = build_alert_message(new_alerts)
    print(message)

    if args.dry_run:
        print("\n[DRY RUN] Skipping Telegram notification.")
    else:
        print("\nSending Telegram alert...")
        success = send_telegram(message)
        if not success:
            print("Failed to send Telegram alert. Check TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID in .env")
            sys.exit(1)

    print("\nDone.")


if __name__ == "__main__":
    main()
