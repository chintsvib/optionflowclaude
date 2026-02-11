#!/usr/bin/env python3
"""
Tool: Multi-Source Confirmation & Opposite Order Detection

Reads 7Day and Floor sheet data, cross-references tickers across sources,
and checks allDay SQLite DB for historical context and opposite orders.

Features:
  1. Multi-Source Confirmation: Same ticker in 7Day + Floor + allDay aligned → HIGH CONVICTION
  2. Opposite Order Detection: New 7Day entry matched by prior allDay entry on opposite side

Usage:
    python tools/multi_source_check.py              # run both checks
    python tools/multi_source_check.py --dry-run    # print without sending Telegram
"""

import os
import sys
import json
import hashlib
import argparse
from datetime import datetime
from zoneinfo import ZoneInfo

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from tools.read_google_sheet import read_sheet
from tools.send_telegram import send_telegram
from tools.monitor_utils import (
    parse_dollar, parse_qty, find_column_index, safe_get,
    format_number, format_qty, filter_new_alerts,
)
from tools.allday_db import query_net_flow, query_net_flow_by_expiry

MULTI_SOURCE_STATE = "multi_source_state.json"


# ---------------------------------------------------------------------------
# Read sheet data (independent of existing monitors)
# ---------------------------------------------------------------------------

def _parse_rows(side_label, headers, data_rows):
    """Parse rows into structured dicts with individual field components."""
    rows = []

    date_idx = find_column_index(headers, "today's date", "order date", "date")
    time_idx = find_column_index(headers, "time", "order time")
    ticker_idx = find_column_index(headers, "ticker", "symbol", "stock")
    xmonth_idx = find_column_index(headers, "xmonth")
    xdate_idx = find_column_index(headers, "xdate")
    expiry_idx = find_column_index(headers, "expiry", "exp", "expiration")
    dte_idx = find_column_index(headers, "dte")
    strike_idx = find_column_index(headers, "strike")
    trade_price_idx = find_column_index(headers, "trade price", "trd $", "trade $")
    target_price_idx = find_column_index(headers, "price target", "trgt", "target price", "price traget")
    call_qty_idx = find_column_index(headers, "calls qty", "call qty", "call quantity")
    call_dollar_idx = find_column_index(headers, "calls $", "call $", "call$", "calls premiums")
    put_qty_idx = find_column_index(headers, "puts qty", "put qty", "put quantity")
    put_dollar_idx = find_column_index(headers, "puts $", "put $", "put$", "puts premiums")
    insights_idx = find_column_index(headers, "order insights", "insights")

    for row in data_rows:
        ticker = safe_get(row, ticker_idx, "").strip()
        if not ticker:
            continue

        xmonth = safe_get(row, xmonth_idx, "").strip()
        xdate = safe_get(row, xdate_idx, "").strip()
        if expiry_idx is not None:
            expiry = safe_get(row, expiry_idx, "")
        elif xmonth and xdate:
            expiry = f"{xmonth} {xdate}"
        else:
            expiry = ""

        strike = safe_get(row, strike_idx, "").strip()
        call_qty = parse_qty(safe_get(row, call_qty_idx)) if call_qty_idx is not None else 0.0
        call_dollar = parse_dollar(safe_get(row, call_dollar_idx)) if call_dollar_idx is not None else 0.0
        put_qty = parse_qty(safe_get(row, put_qty_idx)) if put_qty_idx is not None else 0.0
        put_dollar = parse_dollar(safe_get(row, put_dollar_idx)) if put_dollar_idx is not None else 0.0

        rows.append({
            "side": side_label,
            "ticker": ticker,
            "xmonth": xmonth,
            "xdate": xdate,
            "expiry": expiry,
            "dte": safe_get(row, dte_idx, ""),
            "strike": strike,
            "date": safe_get(row, date_idx, ""),
            "time": safe_get(row, time_idx, ""),
            "trade_price": safe_get(row, trade_price_idx, ""),
            "target_price": safe_get(row, target_price_idx, ""),
            "call_qty": call_qty,
            "call_dollar": call_dollar,
            "put_qty": put_qty,
            "put_dollar": put_dollar,
            "insights": safe_get(row, insights_idx, ""),
            # Row hash for dedup
            "row_hash": hashlib.md5("|".join(str(c) for c in row).encode()).hexdigest(),
        })

    return rows


def read_7day_entries(config):
    """Read all current 7Day entries from Google Sheets."""
    sheet_cfg = config["7Day"]
    header_row = sheet_cfg["header_row"]

    buying_start = sheet_cfg["range_buying"].split(":")[0].rstrip("0123456789")
    buying_end = sheet_cfg["range_buying"].split(":")[1]
    selling_start = sheet_cfg["range_selling"].split(":")[0].rstrip("0123456789")
    selling_end = sheet_cfg["range_selling"].split(":")[1]

    result = {"buying": [], "selling": []}

    # Buying
    buy_range = f"{buying_start}{header_row}:{buying_end}"
    buy_all = read_sheet(sheet_cfg["sheet_url"], sheet_cfg["sheet_name"], buy_range)
    if buy_all and len(buy_all) >= 2:
        result["buying"] = _parse_rows("BUYING", buy_all[0], buy_all[1:])

    # Selling
    sell_range = f"{selling_start}{header_row}:{selling_end}"
    sell_all = read_sheet(sheet_cfg["sheet_url"], sheet_cfg["sheet_name"], sell_range)
    if sell_all and len(sell_all) >= 2:
        result["selling"] = _parse_rows("SELLING", sell_all[0], sell_all[1:])

    return result


def read_floor_entries(config):
    """Read all current Floor entries from Google Sheets."""
    import re
    result = {}

    for cfg_name in ["Floor_SPX_0DTE", "Floor_All"]:
        if cfg_name not in config:
            continue

        sheet_cfg = config[cfg_name]
        range_spec = sheet_cfg["range"]
        header_row = sheet_cfg["header_row"]

        match_start = re.match(r"([A-Z]+)(\d+)", range_spec.split(":")[0])
        match_end = re.match(r"([A-Z]+)(\d+)", range_spec.split(":")[1])
        if not match_start or not match_end:
            continue

        start_col = match_start.group(1)
        end_col = match_end.group(1)

        # Read headers
        header_range = f"{start_col}{header_row}:{end_col}{header_row}"
        header_rows = read_sheet(sheet_cfg["sheet_url"], sheet_cfg["sheet_name"], header_range)
        if not header_rows:
            continue

        # Read data
        data_rows = read_sheet(sheet_cfg["sheet_url"], sheet_cfg["sheet_name"], range_spec)
        if not data_rows:
            continue

        result[cfg_name] = _parse_rows(cfg_name, header_rows[0], data_rows)

    return result


# ---------------------------------------------------------------------------
# Feature 1: Multi-Source Confirmation
# ---------------------------------------------------------------------------

def _ticker_direction(entries):
    """Determine direction for each ticker based on flow.
    Returns dict: {ticker: {"direction": "BULLISH"/"BEARISH", "call_qty": ..., ...}}
    """
    ticker_map = {}
    for e in entries:
        t = e["ticker"]
        if t not in ticker_map:
            ticker_map[t] = {"call_dollar": 0, "put_dollar": 0, "call_qty": 0, "put_qty": 0, "entries": []}
        ticker_map[t]["call_dollar"] += e["call_dollar"]
        ticker_map[t]["put_dollar"] += e["put_dollar"]
        ticker_map[t]["call_qty"] += e["call_qty"]
        ticker_map[t]["put_qty"] += e["put_qty"]
        ticker_map[t]["entries"].append(e)

    result = {}
    for t, data in ticker_map.items():
        # Direction based on which side has more $ flow
        if data["call_dollar"] > data["put_dollar"]:
            direction = "BULLISH"
        elif data["put_dollar"] > data["call_dollar"]:
            direction = "BEARISH"
        else:
            direction = "NEUTRAL"
        result[t] = {**data, "direction": direction}

    return result


def check_multi_source(entries_7day, entries_floor, dry_run=False):
    """
    Check for tickers appearing in multiple sources with aligned direction.
    Queries allDay DB for historical context.
    """
    # Build ticker direction maps for each source
    # 7Day: combine buying entries (buying calls = bullish, buying puts = bearish)
    all_7day = entries_7day.get("buying", []) + entries_7day.get("selling", [])
    dir_7day = _ticker_direction(all_7day)

    # Floor: combine all floor entries
    all_floor = []
    for entries in entries_floor.values():
        all_floor.extend(entries)
    dir_floor = _ticker_direction(all_floor)

    # Find tickers in BOTH 7Day and Floor
    common_tickers = set(dir_7day.keys()) & set(dir_floor.keys())

    if not common_tickers:
        print("  Multi-source: No common tickers between 7Day and Floor.")
        return

    print(f"  Multi-source: {len(common_tickers)} common tickers: {', '.join(sorted(common_tickers))}")

    confirmations = []
    for ticker in sorted(common_tickers):
        d7 = dir_7day[ticker]
        df = dir_floor[ticker]

        # Check if directions align
        if d7["direction"] == "NEUTRAL" or df["direction"] == "NEUTRAL":
            continue

        # Query allDay for historical context
        net_flow = query_net_flow(ticker)

        # Determine historical direction
        if net_flow and (net_flow["net_call_dollar"] != 0 or net_flow["net_put_dollar"] != 0):
            if net_flow["net_call_dollar"] > net_flow["net_put_dollar"]:
                hist_direction = "BULLISH"
            else:
                hist_direction = "BEARISH"
        else:
            hist_direction = None

        # Per-expiry flow breakdown
        expiry_flow = query_net_flow_by_expiry(ticker)

        # All three sources aligned?
        sources_aligned = d7["direction"] == df["direction"]
        hist_aligned = hist_direction == d7["direction"] if hist_direction else None

        if sources_aligned:
            confirmations.append({
                "ticker": ticker,
                "direction": d7["direction"],
                "7day": d7,
                "floor": df,
                "net_flow": net_flow,
                "expiry_flow": expiry_flow[:5],  # Top 5 expiries by flow
                "hist_direction": hist_direction,
                "hist_aligned": hist_aligned,
                # For dedup
                "side": d7["direction"],
                "label": ticker,
                "field": "multi_source",
                "call_dollar": d7["call_dollar"] + df["call_dollar"],
                "call_qty": d7["call_qty"] + df["call_qty"],
                "put_dollar": d7["put_dollar"] + df["put_dollar"],
                "put_qty": d7["put_qty"] + df["put_qty"],
            })

    if not confirmations:
        print("  Multi-source: No aligned confirmations found.")
        return

    # Dedup
    new_confirmations = filter_new_alerts(confirmations, MULTI_SOURCE_STATE)
    if not new_confirmations:
        print("  Multi-source: All confirmations already sent.")
        return

    print(f"  Multi-source: {len(new_confirmations)} NEW confirmations")

    message = _build_multi_source_message(new_confirmations)
    print(message)

    if dry_run:
        print("\n[DRY RUN] Skipping Telegram for multi-source.")
    else:
        send_telegram(message)


def _build_multi_source_message(confirmations):
    """Build Telegram message for multi-source confirmations."""
    now = datetime.now(ZoneInfo("America/New_York")).strftime("%Y-%m-%d %H:%M ET")
    lines = [f"<b>HIGH CONVICTION Alert</b>  ({now})\n"]

    for c in confirmations:
        d7 = c["7day"]
        df = c["floor"]
        nf = c.get("net_flow")

        direction_emoji = "BULLISH" if c["direction"] == "BULLISH" else "BEARISH"

        lines.append(f"  <b>{c['ticker']}</b> — {direction_emoji}\n")

        # 7Day source
        lines.append(f"  7Day: Call Qty {format_qty(d7['call_qty'])} | "
                      f"Call$ {format_number(d7['call_dollar'])} | "
                      f"Put Qty {format_qty(d7['put_qty'])} | "
                      f"Put$ {format_number(d7['put_dollar'])}")

        # Floor source
        lines.append(f"  Floor: Call Qty {format_qty(df['call_qty'])} | "
                      f"Call$ {format_number(df['call_dollar'])} | "
                      f"Put Qty {format_qty(df['put_qty'])} | "
                      f"Put$ {format_number(df['put_dollar'])}")

        # Historical context (overall) + caution flag
        if nf and (nf["buy_count"] > 0 or nf["sell_count"] > 0):
            aligned = c.get("hist_aligned")
            lines.append(
                f"  History: Net Call$ {format_number(abs(nf['net_call_dollar']))} "
                f"({'buy' if nf['net_call_dollar'] >= 0 else 'sell'}) | "
                f"Net Put$ {format_number(abs(nf['net_put_dollar']))} "
                f"({'buy' if nf['net_put_dollar'] >= 0 else 'sell'})"
            )
            lines.append(f"  ({nf['buy_count']} buys, {nf['sell_count']} sells in allDay)")

            if aligned is True:
                lines.append(f"  History ALIGNED with today's flow")
            elif aligned is False:
                # Show opposite-side count as caution
                direction = c["direction"]
                if direction == "BULLISH":
                    opp_count = nf["sell_count"]
                    opp_label = "selling"
                else:
                    opp_count = nf["buy_count"]
                    opp_label = "buying"
                lines.append(
                    f"  CAUTION: Historical flow OPPOSED — "
                    f"{opp_count} prior {opp_label} orders in allDay"
                )

        # Per-expiry flow breakdown
        expiry_flow = c.get("expiry_flow", [])
        if expiry_flow:
            lines.append("  Flow by Expiry:")
            for ef in expiry_flow:
                net_total = ef["net_call_dollar"] + ef["net_put_dollar"]
                sign = "+" if net_total >= 0 else ""
                lines.append(
                    f"    {ef['expiry_label']}: "
                    f"Net Call$ {format_number(abs(ef['net_call_dollar']))} "
                    f"({'buy' if ef['net_call_dollar'] >= 0 else 'sell'}) | "
                    f"Net Put$ {format_number(abs(ef['net_put_dollar']))} "
                    f"({'buy' if ef['net_put_dollar'] >= 0 else 'sell'}) "
                    f"[{ef['direction']}]"
                )

        lines.append("")

    lines.append(f"Total: {len(confirmations)}")
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Feature 2: Opposite Order Detection
# ---------------------------------------------------------------------------

def _row_seen_key(entry):
    """Key for tracking which 7Day rows we've already processed."""
    return hashlib.md5(
        f"{entry['side']}|{entry['row_hash']}".encode()
    ).hexdigest()


def check_opposite_orders(entries_7day, dry_run=False):
    """
    For each new 7Day entry, check allDay for prior opposite-side orders
    matching by same ticker + (qty OR strike/expiry).
    """
    all_rows = entries_7day.get("buying", []) + entries_7day.get("selling", [])
    if not all_rows:
        print("  Opposite orders: No 7Day entries to check.")
        return

    # Track which rows we've already processed (to only check NEW ones)
    from tools.monitor_utils import load_state, save_state
    state = load_state(ROWS_SEEN_STATE)
    seen = set(state["seen"])

    new_rows = []
    for r in all_rows:
        key = _row_seen_key(r)
        if key not in seen:
            new_rows.append(r)
            seen.add(key)

    if not new_rows:
        print("  Opposite orders: No new 7Day rows to check.")
        # Still save state (in case date changed)
        state["seen"] = list(seen)
        save_state(state, ROWS_SEEN_STATE)
        return

    print(f"  Opposite orders: Checking {len(new_rows)} new rows against allDay...")

    matches = []
    for entry in new_rows:
        opposite_side = "SELLING" if entry["side"] == "BUYING" else "BUYING"

        results = query_opposite_orders(
            ticker=entry["ticker"],
            opposite_side=opposite_side,
            call_qty=entry["call_qty"],
            put_qty=entry["put_qty"],
            strike=entry["strike"],
            xmonth=entry["xmonth"],
            xdate=entry["xdate"],
        )

        if results:
            matches.append({
                "new_entry": entry,
                "matched_entries": results[:3],  # Limit to top 3 matches
                # For dedup
                "side": entry["side"],
                "label": f"{entry['ticker']}_{entry['strike']}_{entry['row_hash'][:8]}",
                "field": "opposite",
                "call_dollar": entry["call_dollar"],
                "call_qty": entry["call_qty"],
                "put_dollar": entry["put_dollar"],
                "put_qty": entry["put_qty"],
            })

    # Save seen state
    state["seen"] = list(seen)
    save_state(state, ROWS_SEEN_STATE)

    if not matches:
        print("  Opposite orders: No matches found.")
        return

    # Dedup
    new_matches = filter_new_alerts(matches, OPPOSITE_ORDER_STATE)
    if not new_matches:
        print("  Opposite orders: All matches already sent.")
        return

    print(f"  Opposite orders: {len(new_matches)} NEW matches")

    message = _build_opposite_message(new_matches)
    print(message)

    if dry_run:
        print("\n[DRY RUN] Skipping Telegram for opposite orders.")
    else:
        send_telegram(message)


def _build_opposite_message(matches):
    """Build Telegram message for opposite order matches."""
    now = datetime.now(ZoneInfo("America/New_York")).strftime("%Y-%m-%d %H:%M ET")
    lines = [f"<b>OPPOSITE ORDER Alert</b>  ({now})\n"]

    for m in matches:
        entry = m["new_entry"]
        label = f"{entry['ticker']} {entry['expiry']} {entry['strike']}".strip()

        lines.append(f"  <b>NEW: {label}</b> ({entry['side']})")
        if entry["trade_price"] or entry["target_price"]:
            parts = []
            if entry["trade_price"]:
                parts.append(f"Trade: {entry['trade_price']}")
            if entry["target_price"]:
                parts.append(f"Target: {entry['target_price']}")
            lines.append(f"  {' | '.join(parts)}")

        if entry["call_qty"] > 0 or entry["call_dollar"] > 0:
            lines.append(f"  Call Qty: {format_qty(entry['call_qty'])} | "
                          f"Call$: {format_number(entry['call_dollar'])}")
        if entry["put_qty"] > 0 or entry["put_dollar"] > 0:
            lines.append(f"  Put Qty: {format_qty(entry['put_qty'])} | "
                          f"Put$: {format_number(entry['put_dollar'])}")

        # Show matched prior entries
        for matched in m["matched_entries"]:
            match_label = f"{matched['ticker']} {matched.get('xmonth','')} {matched.get('xdate','')} {matched['strike']}".strip()
            when = f"{matched.get('order_date', '')} {matched.get('order_time', '')}".strip()
            lines.append(f"\n  PRIOR: {match_label} ({matched['side']} on {when})")

            if matched.get("call_qty", 0) > 0:
                lines.append(f"  Call Qty: {format_qty(matched['call_qty'])} | "
                              f"Call$: {format_number(matched.get('call_dollar', 0))}")
            if matched.get("put_qty", 0) > 0:
                lines.append(f"  Put Qty: {format_qty(matched['put_qty'])} | "
                              f"Put$: {format_number(matched.get('put_dollar', 0))}")

            lines.append(f"  <i>Matched: {matched.get('match_reason', '?')}</i>")

        lines.append("")

    lines.append(f"Total matches: {len(matches)}")
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Multi-source confirmation & opposite order detection")
    parser.add_argument("--dry-run", action="store_true",
                        help="Print alerts without sending Telegram message")
    args = parser.parse_args()

    print("=" * 60)
    print(f"MULTI-SOURCE CHECK - {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print("=" * 60)

    # Load config
    config_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "config.json")
    with open(config_path, "r") as f:
        config = json.load(f)

    # Read 7Day data
    print("\nReading 7Day entries...")
    entries_7day = read_7day_entries(config)
    print(f"  Buying: {len(entries_7day.get('buying', []))} rows")
    print(f"  Selling: {len(entries_7day.get('selling', []))} rows")

    # Read Floor data
    print("\nReading Floor entries...")
    entries_floor = read_floor_entries(config)
    for name, entries in entries_floor.items():
        print(f"  {name}: {len(entries)} rows")

    # Multi-Source Confirmation (with historical caution flags)
    print("\n--- Multi-Source Confirmation ---")
    check_multi_source(entries_7day, entries_floor, dry_run=args.dry_run)

    print("\nDone.")


if __name__ == "__main__":
    main()
