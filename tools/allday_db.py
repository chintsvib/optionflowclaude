#!/usr/bin/env python3
"""
Tool: allDay Historical Data — SQLite Database Layer

Reads the allDay ("Prior Orders Data (All)") Google Sheet once per day
and stores all rows in a local SQLite database for fast querying.

Used by multi_source_check.py for:
  - Net flow per ticker (historical buying vs selling)
  - Opposite order detection (same ticker on opposite side)

Usage:
    python tools/allday_db.py              # Load allDay data into SQLite
    python tools/allday_db.py --stats      # Show DB stats
"""

import os
import sys
import json
import sqlite3
import argparse
from datetime import date

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from tools.read_google_sheet import read_sheet
from tools.monitor_utils import (
    parse_dollar, parse_qty, find_column_index, safe_get,
)

DB_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), ".tmp")
DB_PATH = os.path.join(DB_DIR, "allday.db")


# ---------------------------------------------------------------------------
# Database setup
# ---------------------------------------------------------------------------

def get_connection():
    """Get a SQLite connection, creating the DB directory if needed."""
    os.makedirs(DB_DIR, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    """Create the allday_orders table and indexes if they don't exist."""
    conn = get_connection()
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS allday_orders (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
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
            loaded_date TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_allday_ticker ON allday_orders(ticker);
        CREATE INDEX IF NOT EXISTS idx_allday_side_ticker ON allday_orders(side, ticker);

        CREATE TABLE IF NOT EXISTS allday_meta (
            key TEXT PRIMARY KEY,
            value TEXT
        );
    """)
    conn.commit()
    conn.close()


# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------

def is_db_loaded_today():
    """Check if the DB was already loaded today."""
    conn = get_connection()
    try:
        row = conn.execute(
            "SELECT value FROM allday_meta WHERE key='loaded_date'"
        ).fetchone()
        return row is not None and row["value"] == str(date.today())
    except sqlite3.OperationalError:
        return False
    finally:
        conn.close()


def _parse_allday_rows(side_label, headers, data_rows):
    """Parse allDay rows into structured dicts using column auto-detection."""
    entries = []

    date_idx = find_column_index(headers, "order date", "date")
    time_idx = find_column_index(headers, "order time", "time")
    ticker_idx = find_column_index(headers, "ticker", "symbol", "stock")
    xmonth_idx = find_column_index(headers, "xmonth")
    xdate_idx = find_column_index(headers, "xdate")
    xyear_idx = find_column_index(headers, "xyear", "x year")
    dte_idx = find_column_index(headers, "dte")
    strike_idx = find_column_index(headers, "strike")
    trade_price_idx = find_column_index(headers, "trade price", "trd $", "trade $")
    target_price_idx = find_column_index(headers, "price target", "price traget", "trgt", "target price")
    call_qty_idx = find_column_index(headers, "call qty", "calls qty")
    call_dollar_idx = find_column_index(headers, "call $", "call$", "calls $", "call dollar")
    put_qty_idx = find_column_index(headers, "put qty", "puts qty")
    put_dollar_idx = find_column_index(headers, "put $", "put$", "puts $", "put dollar")
    insights_idx = find_column_index(headers, "order insights", "insights")

    print(f"  [{side_label}] Columns: ticker={ticker_idx}, strike={strike_idx}, "
          f"xmonth={xmonth_idx}, xdate={xdate_idx}, call_qty={call_qty_idx}, put_qty={put_qty_idx}")

    for row in data_rows:
        ticker = safe_get(row, ticker_idx, "").strip()
        if not ticker:
            continue

        entries.append({
            "side": side_label,
            "order_date": safe_get(row, date_idx, ""),
            "order_time": safe_get(row, time_idx, ""),
            "ticker": ticker,
            "xmonth": safe_get(row, xmonth_idx, "").strip(),
            "xdate": safe_get(row, xdate_idx, "").strip(),
            "xyear": safe_get(row, xyear_idx, "").strip(),
            "dte": safe_get(row, dte_idx, ""),
            "strike": safe_get(row, strike_idx, "").strip(),
            "trade_price": safe_get(row, trade_price_idx, ""),
            "target_price": safe_get(row, target_price_idx, ""),
            "call_qty": parse_qty(safe_get(row, call_qty_idx)) if call_qty_idx is not None else 0.0,
            "call_dollar": parse_dollar(safe_get(row, call_dollar_idx)) if call_dollar_idx is not None else 0.0,
            "put_qty": parse_qty(safe_get(row, put_qty_idx)) if put_qty_idx is not None else 0.0,
            "put_dollar": parse_dollar(safe_get(row, put_dollar_idx)) if put_dollar_idx is not None else 0.0,
            "insights": safe_get(row, insights_idx, ""),
        })

    return entries


def load_allday_to_db(config=None):
    """Read allDay sheet and load all rows into SQLite. Full reload each time."""
    if config is None:
        config_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "config.json")
        with open(config_path, "r") as f:
            config = json.load(f)

    sheet_cfg = config["allDay"]
    today = str(date.today())

    # Parse range columns
    buying_range = sheet_cfg["range_buying"]  # "A3:Q"
    selling_range = sheet_cfg["range_selling"]  # "S3:AI"
    header_row = sheet_cfg["header_row"]  # 3

    import re
    buy_match = re.match(r"([A-Z]+)", buying_range)
    buy_end = buying_range.split(":")[1]
    sell_match = re.match(r"([A-Z]+)", selling_range)
    sell_end = selling_range.split(":")[1]

    buy_start = buy_match.group(1) if buy_match else "A"
    sell_start = sell_match.group(1) if sell_match else "S"

    # Read buying side
    buy_header_range = f"{buy_start}{header_row}:{buy_end}{header_row}"
    buy_data_range = f"{buy_start}{header_row + 1}:{buy_end}"

    print(f"\nLoading allDay BUYING ({buy_data_range})...")
    buy_headers_rows = read_sheet(sheet_cfg["sheet_url"], sheet_cfg["sheet_name"], buy_header_range)
    if not buy_headers_rows:
        print("  No buying headers found.")
        buy_entries = []
    else:
        buy_headers = buy_headers_rows[0]
        buy_data = read_sheet(sheet_cfg["sheet_url"], sheet_cfg["sheet_name"], buy_data_range)
        print(f"  Rows: {len(buy_data) if buy_data else 0}")
        buy_entries = _parse_allday_rows("BUYING", buy_headers, buy_data or [])
        print(f"  Parsed entries: {len(buy_entries)}")

    # Read selling side
    sell_header_range = f"{sell_start}{header_row}:{sell_end}{header_row}"
    sell_data_range = f"{sell_start}{header_row + 1}:{sell_end}"

    print(f"\nLoading allDay SELLING ({sell_data_range})...")
    sell_headers_rows = read_sheet(sheet_cfg["sheet_url"], sheet_cfg["sheet_name"], sell_header_range)
    if not sell_headers_rows:
        print("  No selling headers found.")
        sell_entries = []
    else:
        sell_headers = sell_headers_rows[0]
        sell_data = read_sheet(sheet_cfg["sheet_url"], sheet_cfg["sheet_name"], sell_data_range)
        print(f"  Rows: {len(sell_data) if sell_data else 0}")
        sell_entries = _parse_allday_rows("SELLING", sell_headers, sell_data or [])
        print(f"  Parsed entries: {len(sell_entries)}")

    # Insert into SQLite
    all_entries = buy_entries + sell_entries
    conn = get_connection()

    # Clear old data and reload
    conn.execute("DELETE FROM allday_orders")
    conn.executemany("""
        INSERT INTO allday_orders
        (side, order_date, order_time, ticker, xmonth, xdate, xyear, dte,
         strike, trade_price, target_price, call_qty, call_dollar,
         put_qty, put_dollar, insights, loaded_date)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, [
        (e["side"], e["order_date"], e["order_time"], e["ticker"],
         e["xmonth"], e["xdate"], e["xyear"], e["dte"],
         e["strike"], e["trade_price"], e["target_price"],
         e["call_qty"], e["call_dollar"], e["put_qty"], e["put_dollar"],
         e["insights"], today)
        for e in all_entries
    ])

    # Update metadata
    conn.execute(
        "INSERT OR REPLACE INTO allday_meta (key, value) VALUES ('loaded_date', ?)",
        (today,)
    )
    conn.commit()
    conn.close()

    print(f"\nLoaded {len(all_entries)} entries into SQLite ({DB_PATH})")
    return len(all_entries)


# ---------------------------------------------------------------------------
# Query functions
# ---------------------------------------------------------------------------

def query_net_flow(ticker):
    """
    Get historical net flow for a ticker using Order Insights to categorize.
    Returns dict with bullish_dollar, bearish_dollar, bullish_count, bearish_count,
    direction.
    """
    conn = get_connection()
    row = conn.execute("""
        SELECT
            COALESCE(SUM(CASE WHEN LOWER(insights) LIKE '%bullish%'
                THEN call_dollar + put_dollar ELSE 0 END), 0) AS bullish_dollar,
            COALESCE(SUM(CASE WHEN LOWER(insights) LIKE '%bearish%'
                THEN call_dollar + put_dollar ELSE 0 END), 0) AS bearish_dollar,
            COALESCE(SUM(CASE WHEN LOWER(insights) LIKE '%bullish%'
                THEN call_qty + put_qty ELSE 0 END), 0) AS bullish_qty,
            COALESCE(SUM(CASE WHEN LOWER(insights) LIKE '%bearish%'
                THEN call_qty + put_qty ELSE 0 END), 0) AS bearish_qty,
            COALESCE(SUM(CASE WHEN LOWER(insights) LIKE '%bullish%'
                THEN 1 ELSE 0 END), 0) AS bullish_count,
            COALESCE(SUM(CASE WHEN LOWER(insights) LIKE '%bearish%'
                THEN 1 ELSE 0 END), 0) AS bearish_count
        FROM allday_orders
        WHERE ticker = ?
    """, (ticker,)).fetchone()
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


def query_net_flow_by_expiry(ticker):
    """
    Get historical net flow for a ticker grouped by expiry (xmonth/xdate/xyear).
    Uses Order Insights to categorize bullish vs bearish.
    Returns list of dicts sorted by net dollar flow (largest first).
    Each dict: {xmonth, xdate, xyear, expiry_label, bullish_dollar, bearish_dollar,
                bullish_qty, bearish_qty, bullish_count, bearish_count, direction}
    """
    conn = get_connection()
    rows = conn.execute("""
        SELECT
            xmonth, xdate, xyear,
            COALESCE(SUM(CASE WHEN LOWER(insights) LIKE '%bullish%'
                THEN call_dollar + put_dollar ELSE 0 END), 0) AS bullish_dollar,
            COALESCE(SUM(CASE WHEN LOWER(insights) LIKE '%bearish%'
                THEN call_dollar + put_dollar ELSE 0 END), 0) AS bearish_dollar,
            COALESCE(SUM(CASE WHEN LOWER(insights) LIKE '%bullish%'
                THEN call_qty + put_qty ELSE 0 END), 0) AS bullish_qty,
            COALESCE(SUM(CASE WHEN LOWER(insights) LIKE '%bearish%'
                THEN call_qty + put_qty ELSE 0 END), 0) AS bearish_qty,
            COALESCE(SUM(CASE WHEN LOWER(insights) LIKE '%bullish%'
                THEN 1 ELSE 0 END), 0) AS bullish_count,
            COALESCE(SUM(CASE WHEN LOWER(insights) LIKE '%bearish%'
                THEN 1 ELSE 0 END), 0) AS bearish_count
        FROM allday_orders
        WHERE ticker = ? AND xmonth != '' AND xdate != ''
        GROUP BY xmonth, xdate, xyear
        ORDER BY ABS(
            COALESCE(SUM(CASE WHEN LOWER(insights) LIKE '%bullish%'
                THEN call_dollar + put_dollar ELSE 0 END), 0) -
            COALESCE(SUM(CASE WHEN LOWER(insights) LIKE '%bearish%'
                THEN call_dollar + put_dollar ELSE 0 END), 0)
        ) DESC
    """, (ticker,)).fetchall()
    conn.close()

    results = []
    for r in rows:
        xm = r["xmonth"]
        xd = r["xdate"]
        xy = r["xyear"]
        expiry_label = f"{xm}/{xd}" + (f"/{xy}" if xy else "")

        bullish = r["bullish_dollar"]
        bearish = r["bearish_dollar"]
        if bullish > bearish:
            direction = "BULLISH"
        elif bearish > bullish:
            direction = "BEARISH"
        else:
            direction = "NEUTRAL"

        results.append({
            "xmonth": xm,
            "xdate": xd,
            "xyear": xy,
            "expiry_label": expiry_label,
            "bullish_dollar": bullish,
            "bearish_dollar": bearish,
            "bullish_qty": r["bullish_qty"],
            "bearish_qty": r["bearish_qty"],
            "bullish_count": r["bullish_count"],
            "bearish_count": r["bearish_count"],
            "direction": direction,
        })

    return results


def query_opposite_orders(ticker, opposite_side, call_qty=None, put_qty=None,
                          strike=None, xmonth=None, xdate=None):
    """
    Find allDay entries on the opposite side that match by:
    - Same ticker AND (same call/put qty [both > 0] OR same strike + xmonth + xdate)

    Returns list of dicts with match_reason.
    """
    conn = get_connection()
    matches = []

    # Build conditions
    conditions = ["ticker = ?", "side = ?"]
    params = [ticker, opposite_side]

    # We'll run separate queries for each match type and combine
    # Match by call_qty
    if call_qty and call_qty > 0:
        rows = conn.execute("""
            SELECT *, 'Same Call Qty' AS match_reason
            FROM allday_orders
            WHERE ticker = ? AND side = ? AND call_qty = ? AND call_qty > 0
        """, (ticker, opposite_side, call_qty)).fetchall()
        matches.extend([dict(r) for r in rows])

    # Match by put_qty
    if put_qty and put_qty > 0:
        rows = conn.execute("""
            SELECT *, 'Same Put Qty' AS match_reason
            FROM allday_orders
            WHERE ticker = ? AND side = ? AND put_qty = ? AND put_qty > 0
        """, (ticker, opposite_side, put_qty)).fetchall()
        matches.extend([dict(r) for r in rows])

    # Match by strike + expiry
    if strike and xmonth and xdate:
        # Normalize strike: strip trailing .0
        strike_norm = strike.rstrip("0").rstrip(".") if "." in strike else strike
        rows = conn.execute("""
            SELECT *, 'Same Strike + Expiry' AS match_reason
            FROM allday_orders
            WHERE ticker = ? AND side = ?
            AND (strike = ? OR strike = ?)
            AND xmonth = ? AND xdate = ?
        """, (ticker, opposite_side, strike, strike_norm, xmonth, xdate)).fetchall()
        matches.extend([dict(r) for r in rows])

    conn.close()

    # Deduplicate by id (a row could match on multiple criteria)
    seen_ids = set()
    unique_matches = []
    for m in matches:
        if m["id"] not in seen_ids:
            seen_ids.add(m["id"])
            unique_matches.append(m)

    return unique_matches


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Load allDay data into SQLite")
    parser.add_argument("--stats", action="store_true", help="Show DB stats")
    args = parser.parse_args()

    init_db()

    if args.stats:
        conn = get_connection()
        total = conn.execute("SELECT COUNT(*) as c FROM allday_orders").fetchone()["c"]
        buying = conn.execute("SELECT COUNT(*) as c FROM allday_orders WHERE side='BUYING'").fetchone()["c"]
        selling = conn.execute("SELECT COUNT(*) as c FROM allday_orders WHERE side='SELLING'").fetchone()["c"]
        tickers = conn.execute("SELECT COUNT(DISTINCT ticker) as c FROM allday_orders").fetchone()["c"]
        loaded = conn.execute("SELECT value FROM allday_meta WHERE key='loaded_date'").fetchone()
        conn.close()
        print(f"allDay DB Stats:")
        print(f"  Total entries:    {total:,}")
        print(f"  Buying entries:   {buying:,}")
        print(f"  Selling entries:  {selling:,}")
        print(f"  Unique tickers:   {tickers}")
        print(f"  Loaded date:      {loaded['value'] if loaded else 'never'}")
        print(f"  DB path:          {DB_PATH}")
    else:
        count = load_allday_to_db()
        print(f"\nDone. {count} entries loaded.")


if __name__ == "__main__":
    main()
