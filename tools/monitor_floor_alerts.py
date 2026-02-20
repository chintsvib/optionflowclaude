#!/usr/bin/env python3
"""
Tool: Monitor Floor Trader Sheets for New Entries
Description: Reads Floor_SPX_0DTE and Floor_All sections from the "Insightful!" sheet
             and sends Telegram alerts for any new rows (no threshold filtering).

Usage:
    python tools/monitor_floor_alerts.py              # defaults
    python tools/monitor_floor_alerts.py --dry-run    # print alerts without sending
"""

import os
import sys
import json
import hashlib
import argparse
from datetime import datetime
from zoneinfo import ZoneInfo

# Add project root to path so we can import sibling tools
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from tools.read_google_sheet import read_sheet
from tools.send_telegram import send_telegram
from tools.monitor_utils import (
    parse_dollar, parse_qty, find_column_index, safe_get,
    format_number, format_qty, filter_new_alerts,
)

STATE_FILE = "floor_alert_state.json"

# Config keys to monitor
FLOOR_CONFIGS = ["Floor_SPX_0DTE", "Floor_SPX_DTEPlus", "Floor_NDX_0DTE", "Floor_NDX_DTEPlus"]


# ---------------------------------------------------------------------------
# Core logic
# ---------------------------------------------------------------------------

def _floor_alert_key(alert):
    """Create a unique key for a floor alert based on row content hash."""
    raw = f"{alert.get('monitor', '')}|{alert.get('row_hash', '')}"
    return hashlib.md5(raw.encode()).hexdigest()


def _row_hash(row):
    """Hash the full row content to detect unique rows."""
    return hashlib.md5("|".join(str(c) for c in row).encode()).hexdigest()


def check_floor_rows(config_name, headers, data_rows):
    """
    Check all non-empty rows in a floor sheet section.
    No threshold — every non-empty row is an alert candidate.

    Returns a list of alert dicts.
    """
    alerts = []

    time_idx = find_column_index(headers, "time")
    ticker_idx = find_column_index(headers, "ticker", "symbol", "stock")
    strike_idx = find_column_index(headers, "strike")
    dte_idx = find_column_index(headers, "dte")
    trade_price_idx = find_column_index(headers, "trade price", "trd $", "trade $")
    target_price_idx = find_column_index(headers, "price target", "trgt", "target price")
    call_dollar_idx = find_column_index(headers, "calls premiums", "calls $", "call$", "call $")
    put_dollar_idx = find_column_index(headers, "puts premiums", "puts $", "put$", "put $")
    call_qty_idx = find_column_index(headers, "calls qty", "call qty")
    put_qty_idx = find_column_index(headers, "puts qty", "put qty")
    insights_idx = find_column_index(headers, "order insights", "insights")

    print(f"\n  [{config_name}] Column mapping:")
    print(f"    Time:         col {time_idx}  ({safe_get(headers, time_idx, '?')})")
    print(f"    Ticker:       col {ticker_idx}  ({safe_get(headers, ticker_idx, '?')})")
    print(f"    Strike:       col {strike_idx}  ({safe_get(headers, strike_idx, '?')})")
    print(f"    DTE:          col {dte_idx}  ({safe_get(headers, dte_idx, '?')})")
    print(f"    Trade Price:  col {trade_price_idx}  ({safe_get(headers, trade_price_idx, '?')})")
    print(f"    Target Price: col {target_price_idx}  ({safe_get(headers, target_price_idx, '?')})")
    print(f"    Call$:        col {call_dollar_idx}  ({safe_get(headers, call_dollar_idx, '?')})")
    print(f"    Put$:         col {put_dollar_idx}  ({safe_get(headers, put_dollar_idx, '?')})")
    print(f"    Call Qty:     col {call_qty_idx}  ({safe_get(headers, call_qty_idx, '?')})")
    print(f"    Put Qty:      col {put_qty_idx}  ({safe_get(headers, put_qty_idx, '?')})")
    print(f"    Insights:     col {insights_idx}  ({safe_get(headers, insights_idx, '?')})")

    for row_offset, row in enumerate(data_rows):
        # Skip empty rows (all cells blank)
        if not any(str(c).strip() for c in row):
            continue

        # Must have at least a ticker to be a valid row
        ticker = safe_get(row, ticker_idx, "").strip()
        if not ticker:
            continue

        row_time = safe_get(row, time_idx, "")
        strike = safe_get(row, strike_idx, "")
        dte = safe_get(row, dte_idx, "")
        trade_price = safe_get(row, trade_price_idx, "")
        target_price = safe_get(row, target_price_idx, "")
        insights = safe_get(row, insights_idx, "")

        # Build label
        label_parts = [ticker]
        if dte:
            label_parts.append(f"{dte}DTE")
        if strike:
            label_parts.append(strike)
        label = " ".join(label_parts)

        call_dollar_val = parse_dollar(safe_get(row, call_dollar_idx)) if call_dollar_idx is not None else 0.0
        put_dollar_val = parse_dollar(safe_get(row, put_dollar_idx)) if put_dollar_idx is not None else 0.0
        call_qty_val = parse_qty(safe_get(row, call_qty_idx)) if call_qty_idx is not None else 0.0
        put_qty_val = parse_qty(safe_get(row, put_qty_idx)) if put_qty_idx is not None else 0.0

        alert = {
            "monitor": config_name,
            "row_hash": _row_hash(row),
            "label": label,
            "time": row_time,
            "trade_price": trade_price,
            "target_price": target_price,
            "call_dollar": call_dollar_val,
            "put_dollar": put_dollar_val,
            "call_qty": call_qty_val,
            "put_qty": put_qty_val,
            "insights": insights,
        }
        alerts.append(alert)

    return alerts


def _alert_header(config_name):
    """Map config name to alert header."""
    if "NDX" in config_name:
        return "NDX Alert"
    return "Insightful Alert"


def build_floor_message(alerts, config_name):
    """Build a Telegram-friendly HTML alert message for floor alerts."""
    header = _alert_header(config_name)
    lines = [f"<b>{header}</b>\n"]

    for a in alerts:
        ticker = a["label"].split()[0] if a.get("label") else "???"
        insights = a.get("insights", "")

        flow_parts = []
        call_qty = a.get("call_qty", 0)
        call_dollar = a.get("call_dollar", 0)
        put_qty = a.get("put_qty", 0)
        put_dollar = a.get("put_dollar", 0)

        if call_qty > 0 or call_dollar > 0:
            flow_parts.append(f"Call Qty: <b>{format_qty(call_qty)}</b>  |  Call$: <b>{format_number(call_dollar)}</b>")
        if put_qty > 0 or put_dollar > 0:
            flow_parts.append(f"Put Qty: <b>{format_qty(put_qty)}</b>  |  Put$: <b>{format_number(put_dollar)}</b>")
        flow_line = "\n".join(flow_parts) + "\n" if flow_parts else ""

        lines.append(
            f"<b>{ticker}</b> {insights}\n"
            f"{flow_line}"
        )

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Monitor Floor Trader sheets for new entries")
    parser.add_argument("--dry-run", action="store_true",
                        help="Print alerts without sending Telegram message")
    args = parser.parse_args()

    print("=" * 60)
    print(f"FLOOR TRADER MONITOR - {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print("Mode: No threshold — alert on any new row")
    print("=" * 60)

    # Load config
    config_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "config.json")
    with open(config_path, "r") as f:
        config = json.load(f)

    all_alerts = []

    for cfg_name in FLOOR_CONFIGS:
        if cfg_name not in config:
            print(f"\n  [{cfg_name}] Not found in config.json — skipping.")
            continue

        sheet_cfg = config[cfg_name]
        range_spec = sheet_cfg["range"]
        header_row = sheet_cfg["header_row"]

        # Build header range: same columns as data range but at header_row
        # e.g. range="A4:K29" -> header range = "A3:K3" (header_row=3)
        range_parts = range_spec.replace(" ", "")
        # Extract column letters and row numbers
        import re
        match_start = re.match(r"([A-Z]+)(\d+)", range_parts.split(":")[0])
        match_end = re.match(r"([A-Z]+)(\d+)", range_parts.split(":")[1])
        if not match_start or not match_end:
            print(f"\n  [{cfg_name}] Could not parse range '{range_spec}' — skipping.")
            continue

        start_col = match_start.group(1)
        end_col = match_end.group(1)

        # Read header row
        header_range = f"{start_col}{header_row}:{end_col}{header_row}"
        print(f"\nReading {cfg_name} headers ({header_range})...")
        header_rows = read_sheet(sheet_cfg["sheet_url"], sheet_cfg["sheet_name"], header_range)

        if not header_rows:
            print(f"  No headers found for {cfg_name}.")
            continue

        headers = header_rows[0]
        print(f"  Headers: {headers}")

        # Read data rows
        print(f"Reading {cfg_name} data ({range_spec})...")
        data_rows = read_sheet(sheet_cfg["sheet_url"], sheet_cfg["sheet_name"], range_spec)

        if not data_rows:
            print(f"  No data rows found for {cfg_name}.")
            continue

        print(f"  Data rows: {len(data_rows)}")

        # Check all rows (no threshold)
        section_alerts = check_floor_rows(cfg_name, headers, data_rows)
        print(f"  Rows with data: {len(section_alerts)}")

        if section_alerts:
            # Deduplicate: only send NEW alerts
            new_section = filter_new_alerts(section_alerts, STATE_FILE, key_fn=_floor_alert_key)
            print(f"  NEW alerts: {len(new_section)} (of {len(section_alerts)} total)")

            if new_section:
                # Send per-section message
                message = build_floor_message(new_section, cfg_name)
                print(message)

                if args.dry_run:
                    print(f"\n[DRY RUN] Skipping Telegram for {cfg_name}.")
                else:
                    print(f"\nSending Telegram alert for {cfg_name}...")
                    success = send_telegram(message)
                    if not success:
                        print(f"Failed to send Telegram alert for {cfg_name}.")
            else:
                print(f"  No NEW alerts for {cfg_name} since last run.")

    print("\nDone.")


if __name__ == "__main__":
    main()
