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
import argparse
from datetime import datetime
from zoneinfo import ZoneInfo

# Add project root to path so we can import sibling tools
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from tools.read_google_sheet import get_credentials, extract_sheet_id, read_sheet
from tools.send_telegram import send_telegram
from tools.monitor_utils import (
    parse_dollar, parse_qty, find_column_index, safe_get,
    format_number, format_qty, filter_new_alerts,
)

STATE_FILE = "7day_alert_state.json"


# ---------------------------------------------------------------------------
# Core logic
# ---------------------------------------------------------------------------

def check_side(side_label, headers, data_rows, dollar_threshold, qty_threshold):
    """
    Check one side (buying or selling) for threshold breaches.

    Returns a list of alert dicts.
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
    call_dollar_idx = find_column_index(headers, "calls $", "call$", "call $", "call dollar", "calls premiums")
    put_dollar_idx = find_column_index(headers, "puts $", "put$", "put $", "put dollar", "puts premiums")
    call_qty_idx = find_column_index(headers, "calls qty", "call qty", "call quantity", "# calls", "call vol")
    put_qty_idx = find_column_index(headers, "puts qty", "put qty", "put quantity", "# puts", "put vol")
    insights_idx = find_column_index(headers, "order insights", "insights")
    trade_price_idx = find_column_index(headers, "trade price", "trd $", "trade $")
    target_price_idx = find_column_index(headers, "price target", "trgt", "target price")

    print(f"\n  [{side_label}] Column mapping:")
    print(f"    Ticker:       col {ticker_idx}  ({safe_get(headers, ticker_idx, '?')})")
    print(f"    Expiry:       col {expiry_idx} / xMonth={xmonth_idx} xDate={xdate_idx}")
    print(f"    Strike:       col {strike_idx}  ({safe_get(headers, strike_idx, '?')})")
    print(f"    Call$:        col {call_dollar_idx}  ({safe_get(headers, call_dollar_idx, '?')})")
    print(f"    Put$:         col {put_dollar_idx}  ({safe_get(headers, put_dollar_idx, '?')})")
    print(f"    Call Qty:     col {call_qty_idx}  ({safe_get(headers, call_qty_idx, '?')})")
    print(f"    Put Qty:      col {put_qty_idx}  ({safe_get(headers, put_qty_idx, '?')})")
    print(f"    Trade Price:  col {trade_price_idx}  ({safe_get(headers, trade_price_idx, '?')})")
    print(f"    Target Price: col {target_price_idx}  ({safe_get(headers, target_price_idx, '?')})")

    for row_offset, row in enumerate(data_rows):
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
        trade_price = safe_get(row, trade_price_idx, "")
        target_price = safe_get(row, target_price_idx, "")

        # Parse all four values for this row
        call_dollar_val = parse_dollar(safe_get(row, call_dollar_idx)) if call_dollar_idx is not None else 0.0
        put_dollar_val = parse_dollar(safe_get(row, put_dollar_idx)) if put_dollar_idx is not None else 0.0
        call_qty_val = parse_qty(safe_get(row, call_qty_idx)) if call_qty_idx is not None else 0.0
        put_qty_val = parse_qty(safe_get(row, put_qty_idx)) if put_qty_idx is not None else 0.0

        # Common fields for each alert from this row
        base = {
            "side": side_label, "row": sheet_row, "label": label,
            "date": row_date, "time": row_time, "insights": insights,
            "trade_price": trade_price, "target_price": target_price,
            "call_dollar": call_dollar_val, "put_dollar": put_dollar_val,
            "call_qty": call_qty_val, "put_qty": put_qty_val,
        }

        # Check Call$ or Call Qty
        if call_dollar_val > dollar_threshold or call_qty_val > qty_threshold:
            alerts.append({**base, "field": "Call", "value": 0, "threshold": 0})

        # Check Put$ or Put Qty
        if put_dollar_val > dollar_threshold or put_qty_val > qty_threshold:
            alerts.append({**base, "field": "Put", "value": 0, "threshold": 0})

    return alerts


def build_alert_message(alerts):
    """Build a Telegram-friendly HTML alert message."""
    lines = [f"<b>7DTE Alert</b>\n"]

    for a in alerts:
        side_type = a["field"]  # "Call" or "Put"

        if side_type == "Call":
            dollar_val = a.get("call_dollar", 0)
            qty_val = a.get("call_qty", 0)
        else:
            dollar_val = a.get("put_dollar", 0)
            qty_val = a.get("put_qty", 0)

        ticker = a["label"].split()[0] if a.get("label") else "???"
        insights = a.get("insights", "")

        lines.append(
            f"<b>{ticker}</b> {insights}\n"
            f"{side_type} Qty: <b>{format_qty(qty_val)}</b>  |  "
            f"{side_type}$: <b>{format_number(dollar_val)}</b>\n"
        )

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Monitor 7Day sheet for large option flows")
    parser.add_argument("--dollar", type=float, default=500_000,
                        help="Dollar threshold for Call$/Put$ (default: 500000)")
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
    new_alerts = filter_new_alerts(all_alerts, STATE_FILE)

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
