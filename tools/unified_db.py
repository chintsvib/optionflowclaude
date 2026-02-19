#!/usr/bin/env python3
"""
Tool: Unified Flow Database — SQLite

Loads data from ALL sheets (allDay, 7Day, Floor) into a single SQLite database
for cross-source querying. Uses Order Insights to categorize bullish vs bearish.

Usage:
    python tools/unified_db.py                 # Load all sources
    python tools/unified_db.py --stats         # Show DB stats
    python tools/unified_db.py --source allDay # Load only allDay
"""

import os
import sys
import json
import re
import sqlite3
import argparse
from datetime import date, timedelta

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from tools.read_google_sheet import read_sheet
from tools.monitor_utils import (
    parse_dollar, parse_qty, find_column_index, safe_get,
)

DB_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), ".tmp")
DB_PATH = os.path.join(DB_DIR, "unified.db")

# All Floor config keys
FLOOR_CONFIGS = ["Floor_SPX_0DTE", "Floor_SPX_DTEPlus", "Floor_NDX_0DTE", "Floor_NDX_DTEPlus", "Floor_All", "Floor_All_21DTE"]


# ---------------------------------------------------------------------------
# Database setup
# ---------------------------------------------------------------------------

def get_connection():
    os.makedirs(DB_DIR, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    conn = get_connection()
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS flow_orders (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            source TEXT,
            side TEXT,
            order_date TEXT,
            order_time TEXT,
            ticker TEXT,
            xmonth TEXT,
            xdate TEXT,
            xyear TEXT,
            dte TEXT,
            strike TEXT,
            trade_price TEXT,
            target_price TEXT,
            call_qty REAL,
            call_dollar REAL,
            put_qty REAL,
            put_dollar REAL,
            insights TEXT,
            direction TEXT,
            loaded_date TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_flow_ticker ON flow_orders(ticker);
        CREATE INDEX IF NOT EXISTS idx_flow_source ON flow_orders(source);
        CREATE INDEX IF NOT EXISTS idx_flow_direction ON flow_orders(direction);
        CREATE INDEX IF NOT EXISTS idx_flow_source_ticker ON flow_orders(source, ticker);

        CREATE TABLE IF NOT EXISTS flow_meta (
            key TEXT PRIMARY KEY,
            value TEXT
        );
    """)
    conn.commit()
    conn.close()


# ---------------------------------------------------------------------------
# Shared parser
# ---------------------------------------------------------------------------

def _parse_rows(source, side_label, headers, data_rows):
    """Parse sheet rows into dicts with auto-detected columns."""
    entries = []

    date_idx = find_column_index(headers, "today's date", "order date", "date")
    time_idx = find_column_index(headers, "time", "order time")
    ticker_idx = find_column_index(headers, "ticker", "symbol", "stock")
    xmonth_idx = find_column_index(headers, "xmonth")
    xdate_idx = find_column_index(headers, "xdate")
    xyear_idx = find_column_index(headers, "xyear", "x year")
    dte_idx = find_column_index(headers, "dte")
    strike_idx = find_column_index(headers, "strike")
    trade_price_idx = find_column_index(headers, "trade price", "trd $", "trade $")
    target_price_idx = find_column_index(headers, "price target", "price traget", "trgt", "target price")
    call_qty_idx = find_column_index(headers, "calls qty", "call qty", "call quantity")
    call_dollar_idx = find_column_index(headers, "calls $", "call $", "call$", "calls premiums")
    put_qty_idx = find_column_index(headers, "puts qty", "put qty", "put quantity")
    put_dollar_idx = find_column_index(headers, "puts $", "put $", "put$", "puts premiums")
    insights_idx = find_column_index(headers, "order insights", "insights")

    for row in data_rows:
        ticker = safe_get(row, ticker_idx, "").strip()
        if not ticker:
            continue

        insights = safe_get(row, insights_idx, "")
        insights_lower = insights.lower()
        if "bullish" in insights_lower:
            direction = "BULLISH"
        elif "bearish" in insights_lower:
            direction = "BEARISH"
        else:
            direction = ""

        xmonth = safe_get(row, xmonth_idx, "").strip()
        xdate = safe_get(row, xdate_idx, "").strip()
        xyear = safe_get(row, xyear_idx, "").strip()
        dte_val = safe_get(row, dte_idx, "").strip()

        # If no xmonth/xdate but DTE is available, compute expiry from today + DTE
        if not xmonth and not xdate and dte_val:
            try:
                expiry_date = date.today() + timedelta(days=int(dte_val))
                xmonth = str(expiry_date.month)
                xdate = str(expiry_date.day)
                xyear = str(expiry_date.year % 100)
            except (ValueError, TypeError):
                pass

        # Normalize: strip leading zeros, default year to current
        if xmonth:
            try:
                xmonth = str(int(xmonth))
            except ValueError:
                pass
        if xdate:
            try:
                xdate = str(int(xdate))
            except ValueError:
                pass
        if xmonth and xdate and not xyear:
            xyear = str(date.today().year % 100)

        entries.append({
            "source": source,
            "side": side_label,
            "order_date": safe_get(row, date_idx, ""),
            "order_time": safe_get(row, time_idx, ""),
            "ticker": ticker,
            "xmonth": xmonth,
            "xdate": xdate,
            "xyear": xyear,
            "dte": dte_val,
            "strike": safe_get(row, strike_idx, "").strip(),
            "trade_price": safe_get(row, trade_price_idx, ""),
            "target_price": safe_get(row, target_price_idx, ""),
            "call_qty": parse_qty(safe_get(row, call_qty_idx)) if call_qty_idx is not None else 0.0,
            "call_dollar": parse_dollar(safe_get(row, call_dollar_idx)) if call_dollar_idx is not None else 0.0,
            "put_qty": parse_qty(safe_get(row, put_qty_idx)) if put_qty_idx is not None else 0.0,
            "put_dollar": parse_dollar(safe_get(row, put_dollar_idx)) if put_dollar_idx is not None else 0.0,
            "insights": insights,
            "direction": direction,
        })

    return entries


# ---------------------------------------------------------------------------
# Loaders
# ---------------------------------------------------------------------------

def _load_allday(config):
    """Load allDay buying + selling."""
    sheet_cfg = config["allDay"]
    buying_range = sheet_cfg["range_buying"]
    selling_range = sheet_cfg["range_selling"]
    header_row = sheet_cfg["header_row"]

    buy_start = re.match(r"([A-Z]+)", buying_range).group(1)
    buy_end = buying_range.split(":")[1]
    sell_start = re.match(r"([A-Z]+)", selling_range).group(1)
    sell_end = selling_range.split(":")[1]

    entries = []

    # Buying
    print(f"\n  allDay BUYING...")
    hdr = read_sheet(sheet_cfg["sheet_url"], sheet_cfg["sheet_name"],
                     f"{buy_start}{header_row}:{buy_end}{header_row}")
    if hdr:
        data = read_sheet(sheet_cfg["sheet_url"], sheet_cfg["sheet_name"],
                          f"{buy_start}{header_row + 1}:{buy_end}")
        print(f"    Rows: {len(data) if data else 0}")
        entries.extend(_parse_rows("allDay", "BUYING", hdr[0], data or []))

    # Selling
    print(f"  allDay SELLING...")
    hdr = read_sheet(sheet_cfg["sheet_url"], sheet_cfg["sheet_name"],
                     f"{sell_start}{header_row}:{sell_end}{header_row}")
    if hdr:
        data = read_sheet(sheet_cfg["sheet_url"], sheet_cfg["sheet_name"],
                          f"{sell_start}{header_row + 1}:{sell_end}")
        print(f"    Rows: {len(data) if data else 0}")
        entries.extend(_parse_rows("allDay", "SELLING", hdr[0], data or []))

    print(f"  allDay total: {len(entries)} entries")
    return entries


def _load_7day(config):
    """Load 7Day buying + selling."""
    sheet_cfg = config["7Day"]
    header_row = sheet_cfg["header_row"]

    buying_start = sheet_cfg["range_buying"].split(":")[0].rstrip("0123456789")
    buying_end = sheet_cfg["range_buying"].split(":")[1]
    selling_start = sheet_cfg["range_selling"].split(":")[0].rstrip("0123456789")
    selling_end = sheet_cfg["range_selling"].split(":")[1]

    entries = []

    # Buying
    print(f"\n  7Day BUYING...")
    buy_range = f"{buying_start}{header_row}:{buying_end}"
    buy_all = read_sheet(sheet_cfg["sheet_url"], sheet_cfg["sheet_name"], buy_range)
    if buy_all and len(buy_all) >= 2:
        parsed = _parse_rows("7Day", "BUYING", buy_all[0], buy_all[1:])
        entries.extend(parsed)
        print(f"    Entries: {len(parsed)}")

    # Selling
    print(f"  7Day SELLING...")
    sell_range = f"{selling_start}{header_row}:{selling_end}"
    sell_all = read_sheet(sheet_cfg["sheet_url"], sheet_cfg["sheet_name"], sell_range)
    if sell_all and len(sell_all) >= 2:
        parsed = _parse_rows("7Day", "SELLING", sell_all[0], sell_all[1:])
        entries.extend(parsed)
        print(f"    Entries: {len(parsed)}")

    print(f"  7Day total: {len(entries)} entries")
    return entries


def _load_floor(config):
    """Load all Floor sections."""
    entries = []

    for cfg_name in FLOOR_CONFIGS:
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

        print(f"\n  {cfg_name}...")
        header_range = f"{start_col}{header_row}:{end_col}{header_row}"
        hdr = read_sheet(sheet_cfg["sheet_url"], sheet_cfg["sheet_name"], header_range)
        if not hdr:
            print(f"    No headers found.")
            continue

        data = read_sheet(sheet_cfg["sheet_url"], sheet_cfg["sheet_name"], range_spec)
        if not data:
            print(f"    No data found.")
            continue

        parsed = _parse_rows(cfg_name, cfg_name, hdr[0], data)
        entries.extend(parsed)
        print(f"    Entries: {len(parsed)}")

    print(f"  Floor total: {len(entries)} entries")
    return entries


def _insert_entries(entries, source_filter=None):
    """Insert entries into the DB, clearing old data for the given source(s)."""
    conn = get_connection()
    today = str(date.today())

    if source_filter:
        # Clear only specific source
        conn.execute("DELETE FROM flow_orders WHERE source = ?", (source_filter,))
    else:
        # Clear all
        conn.execute("DELETE FROM flow_orders")

    conn.executemany("""
        INSERT INTO flow_orders
        (source, side, order_date, order_time, ticker, xmonth, xdate, xyear, dte,
         strike, trade_price, target_price, call_qty, call_dollar,
         put_qty, put_dollar, insights, direction, loaded_date)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, [
        (e["source"], e["side"], e["order_date"], e["order_time"], e["ticker"],
         e["xmonth"], e["xdate"], e["xyear"], e["dte"],
         e["strike"], e["trade_price"], e["target_price"],
         e["call_qty"], e["call_dollar"], e["put_qty"], e["put_dollar"],
         e["insights"], e["direction"], today)
        for e in entries
    ])

    conn.execute(
        "INSERT OR REPLACE INTO flow_meta (key, value) VALUES ('loaded_date', ?)",
        (today,)
    )
    conn.commit()
    conn.close()


def load_all(config=None, sources=None):
    """Load data from specified sources (or all) into unified DB."""
    if config is None:
        config_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "config.json")
        with open(config_path, "r") as f:
            config = json.load(f)

    if sources is None:
        sources = ["allDay", "7Day", "Floor"]

    all_entries = []

    if "allDay" in sources:
        all_entries.extend(_load_allday(config))
    if "7Day" in sources:
        all_entries.extend(_load_7day(config))
    if "Floor" in sources:
        all_entries.extend(_load_floor(config))

    # If loading a single source, only clear that source
    source_filter = None
    if len(sources) == 1:
        source_filter = sources[0]
        # For Floor, the source names are the config names, not "Floor"
        if source_filter == "Floor":
            source_filter = None  # clear all floor entries
            conn = get_connection()
            for cfg_name in FLOOR_CONFIGS:
                conn.execute("DELETE FROM flow_orders WHERE source = ?", (cfg_name,))
            conn.commit()
            conn.close()
            # Insert without clearing again
            conn = get_connection()
            today = str(date.today())
            conn.executemany("""
                INSERT INTO flow_orders
                (source, side, order_date, order_time, ticker, xmonth, xdate, xyear, dte,
                 strike, trade_price, target_price, call_qty, call_dollar,
                 put_qty, put_dollar, insights, direction, loaded_date)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, [
                (e["source"], e["side"], e["order_date"], e["order_time"], e["ticker"],
                 e["xmonth"], e["xdate"], e["xyear"], e["dte"],
                 e["strike"], e["trade_price"], e["target_price"],
                 e["call_qty"], e["call_dollar"], e["put_qty"], e["put_dollar"],
                 e["insights"], e["direction"], today)
                for e in all_entries
            ])
            conn.execute(
                "INSERT OR REPLACE INTO flow_meta (key, value) VALUES ('loaded_date', ?)",
                (today,)
            )
            conn.commit()
            conn.close()
            print(f"\nLoaded {len(all_entries)} entries into unified DB ({DB_PATH})")
            return len(all_entries)

    _insert_entries(all_entries, source_filter)
    print(f"\nLoaded {len(all_entries)} entries into unified DB ({DB_PATH})")
    return len(all_entries)


# ---------------------------------------------------------------------------
# Query functions
# ---------------------------------------------------------------------------

def query_net_flow(ticker, source=None):
    """Net bullish/bearish flow for a ticker, optionally filtered by source."""
    conn = get_connection()
    where = "ticker = ?"
    params = [ticker]
    if source:
        where += " AND source = ?"
        params.append(source)

    row = conn.execute(f"""
        SELECT
            COALESCE(SUM(CASE WHEN direction='BULLISH'
                THEN call_dollar + put_dollar ELSE 0 END), 0) AS bullish_dollar,
            COALESCE(SUM(CASE WHEN direction='BEARISH'
                THEN call_dollar + put_dollar ELSE 0 END), 0) AS bearish_dollar,
            COALESCE(SUM(CASE WHEN direction='BULLISH'
                THEN call_qty + put_qty ELSE 0 END), 0) AS bullish_qty,
            COALESCE(SUM(CASE WHEN direction='BEARISH'
                THEN call_qty + put_qty ELSE 0 END), 0) AS bearish_qty,
            COALESCE(SUM(CASE WHEN direction='BULLISH' THEN 1 ELSE 0 END), 0) AS bullish_count,
            COALESCE(SUM(CASE WHEN direction='BEARISH' THEN 1 ELSE 0 END), 0) AS bearish_count
        FROM flow_orders
        WHERE {where}
    """, params).fetchone()
    conn.close()

    if row is None:
        return None

    bullish = row["bullish_dollar"]
    bearish = row["bearish_dollar"]
    if bullish > bearish:
        direction = "BULLISH"
    elif bearish > bullish:
        direction = "BEARISH"
    else:
        direction = "NEUTRAL"

    return {
        "bullish_dollar": bullish,
        "bearish_dollar": bearish,
        "bullish_qty": row["bullish_qty"],
        "bearish_qty": row["bearish_qty"],
        "bullish_count": row["bullish_count"],
        "bearish_count": row["bearish_count"],
        "direction": direction,
    }


def query_net_flow_by_source(ticker):
    """Net bullish/bearish flow for a ticker broken down by source."""
    conn = get_connection()
    rows = conn.execute("""
        SELECT
            source,
            COALESCE(SUM(CASE WHEN direction='BULLISH'
                THEN call_dollar + put_dollar ELSE 0 END), 0) AS bullish_dollar,
            COALESCE(SUM(CASE WHEN direction='BEARISH'
                THEN call_dollar + put_dollar ELSE 0 END), 0) AS bearish_dollar,
            COALESCE(SUM(CASE WHEN direction='BULLISH'
                THEN call_qty + put_qty ELSE 0 END), 0) AS bullish_qty,
            COALESCE(SUM(CASE WHEN direction='BEARISH'
                THEN call_qty + put_qty ELSE 0 END), 0) AS bearish_qty,
            COALESCE(SUM(CASE WHEN direction='BULLISH' THEN 1 ELSE 0 END), 0) AS bullish_count,
            COALESCE(SUM(CASE WHEN direction='BEARISH' THEN 1 ELSE 0 END), 0) AS bearish_count
        FROM flow_orders
        WHERE ticker = ?
        GROUP BY source
        ORDER BY bullish_dollar + bearish_dollar DESC
    """, (ticker,)).fetchall()
    conn.close()

    results = []
    for r in rows:
        bullish = r["bullish_dollar"]
        bearish = r["bearish_dollar"]
        if bullish > bearish:
            direction = "BULLISH"
        elif bearish > bullish:
            direction = "BEARISH"
        else:
            direction = "NEUTRAL"

        results.append({
            "source": r["source"],
            "bullish_dollar": bullish,
            "bearish_dollar": bearish,
            "bullish_qty": r["bullish_qty"],
            "bearish_qty": r["bearish_qty"],
            "bullish_count": r["bullish_count"],
            "bearish_count": r["bearish_count"],
            "direction": direction,
        })

    return results


def print_stats():
    """Print database statistics."""
    conn = get_connection()

    total = conn.execute("SELECT COUNT(*) as c FROM flow_orders").fetchone()["c"]
    if total == 0:
        print("Unified DB is empty. Run: python tools/unified_db.py")
        conn.close()
        return

    print(f"\n{'=' * 60}")
    print(f"  Unified Flow Database — {DB_PATH}")
    print(f"  Total entries: {total:,}")
    print(f"{'=' * 60}\n")

    # By source
    print("  By Source:")
    rows = conn.execute("""
        SELECT source, COUNT(*) as cnt,
               SUM(CASE WHEN direction='BULLISH' THEN 1 ELSE 0 END) as bull,
               SUM(CASE WHEN direction='BEARISH' THEN 1 ELSE 0 END) as bear
        FROM flow_orders GROUP BY source ORDER BY cnt DESC
    """).fetchall()
    for r in rows:
        print(f"    {r['source']:<20} {r['cnt']:>6} entries  "
              f"({r['bull']} bullish, {r['bear']} bearish)")

    # Top tickers
    print("\n  Top 10 Tickers (by entry count):")
    rows = conn.execute("""
        SELECT ticker, COUNT(*) as cnt,
               COUNT(DISTINCT source) as sources,
               SUM(CASE WHEN direction='BULLISH' THEN 1 ELSE 0 END) as bull,
               SUM(CASE WHEN direction='BEARISH' THEN 1 ELSE 0 END) as bear
        FROM flow_orders
        GROUP BY ticker ORDER BY cnt DESC LIMIT 10
    """).fetchall()
    for r in rows:
        dir_label = "BULLISH" if r["bull"] > r["bear"] else "BEARISH"
        print(f"    {r['ticker']:<8} {r['cnt']:>5} entries  "
              f"{r['sources']} sources  {dir_label} ({r['bull']}B/{r['bear']}R)")

    # Load date
    meta = conn.execute("SELECT value FROM flow_meta WHERE key='loaded_date'").fetchone()
    if meta:
        print(f"\n  Last loaded: {meta['value']}")

    conn.close()
    print()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Load all sheet data into unified SQLite database",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Sources: allDay, 7Day, Floor
Examples:
  python tools/unified_db.py                  # load all sources
  python tools/unified_db.py --source allDay  # load only allDay
  python tools/unified_db.py --stats          # show DB stats
        """,
    )
    parser.add_argument("--source", choices=["allDay", "7Day", "Floor"],
                        help="Load only a specific source")
    parser.add_argument("--stats", action="store_true",
                        help="Show database statistics")
    args = parser.parse_args()

    init_db()

    if args.stats:
        print_stats()
        return

    sources = [args.source] if args.source else None
    print(f"Loading sources: {sources or ['allDay', '7Day', 'Floor']}")
    load_all(sources=sources)


if __name__ == "__main__":
    main()
