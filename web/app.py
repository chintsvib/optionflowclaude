#!/usr/bin/env python3
"""
OptionFlow Web Dashboard — FastAPI Backend

Serves a single-page dashboard and provides JSON API endpoints
that query the unified SQLite database.

Usage:
    python -m uvicorn web.app:app --reload --port 8000
"""

import os
import sys
import threading
from datetime import datetime, timedelta

from fastapi import FastAPI, Query
from fastapi.responses import FileResponse, JSONResponse

# Allow imports from project root
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from tools.unified_db import get_connection, init_db, load_all

app = FastAPI(title="OptionFlow Dashboard")

# Ensure DB exists on startup
init_db()

# ---------------------------------------------------------------------------
# Background refresh management
# ---------------------------------------------------------------------------

_refresh_lock = threading.Lock()
_refresh_status = {"running": False, "last_result": None, "last_error": None}


def _run_refresh():
    """Background thread: reload all sources from Google Sheets."""
    try:
        init_db()
        count = load_all()
        _refresh_status["last_result"] = {
            "entries_loaded": count,
            "timestamp": datetime.now().isoformat(),
        }
        _refresh_status["last_error"] = None
    except Exception as e:
        _refresh_status["last_error"] = str(e)
    finally:
        _refresh_status["running"] = False


@app.post("/api/refresh")
async def refresh_db():
    with _refresh_lock:
        if _refresh_status["running"]:
            return {"status": "already_running"}
        _refresh_status["running"] = True

    t = threading.Thread(target=_run_refresh, daemon=True)
    t.start()
    return {"status": "started"}


@app.get("/api/refresh/status")
async def refresh_status():
    return _refresh_status


# ---------------------------------------------------------------------------
# Database stats
# ---------------------------------------------------------------------------

@app.get("/api/db/stats")
async def db_stats():
    conn = get_connection()
    try:
        total = conn.execute("SELECT COUNT(*) as c FROM flow_orders").fetchone()["c"]
    except Exception:
        return {"total_entries": 0, "distinct_tickers": 0, "loaded_date": None, "sources": []}

    if total == 0:
        conn.close()
        return {"total_entries": 0, "distinct_tickers": 0, "loaded_date": None, "sources": []}

    tickers = conn.execute("SELECT COUNT(DISTINCT ticker) as c FROM flow_orders").fetchone()["c"]

    sources = []
    for r in conn.execute("""
        SELECT source, COUNT(*) as cnt,
               SUM(CASE WHEN direction='BULLISH' THEN 1 ELSE 0 END) as bull,
               SUM(CASE WHEN direction='BEARISH' THEN 1 ELSE 0 END) as bear
        FROM flow_orders GROUP BY source ORDER BY cnt DESC
    """).fetchall():
        sources.append({"source": r["source"], "count": r["cnt"],
                        "bullish": r["bull"], "bearish": r["bear"]})

    meta = conn.execute("SELECT value FROM flow_meta WHERE key='loaded_date'").fetchone()
    loaded_date = meta["value"] if meta else None

    conn.close()
    return {
        "total_entries": total,
        "distinct_tickers": tickers,
        "loaded_date": loaded_date,
        "sources": sources,
    }


# ---------------------------------------------------------------------------
# Ticker autocomplete
# ---------------------------------------------------------------------------

@app.get("/api/tickers")
async def get_tickers(q: str = Query("", min_length=0)):
    conn = get_connection()
    if q:
        rows = conn.execute(
            "SELECT DISTINCT ticker FROM flow_orders WHERE ticker LIKE ? ORDER BY ticker LIMIT 50",
            (f"{q.upper()}%",),
        ).fetchall()
    else:
        rows = conn.execute(
            "SELECT DISTINCT ticker FROM flow_orders ORDER BY ticker LIMIT 50"
        ).fetchall()
    conn.close()
    return {"tickers": [r["ticker"] for r in rows]}


# ---------------------------------------------------------------------------
# Main flow query
# ---------------------------------------------------------------------------

def _parse_order_date(date_str):
    """Parse M/D/YY date string to date object."""
    try:
        parts = date_str.strip().split("/")
        if len(parts) == 3:
            m, d, y = int(parts[0]), int(parts[1]), int(parts[2])
            year = 2000 + y if y < 100 else y
            return datetime(year, m, d).date()
    except (ValueError, IndexError):
        pass
    return None


def _parse_expiry(xmonth, xdate, xyear):
    """Parse xmonth/xdate/xyear into a sortable date."""
    try:
        m = int(xmonth)
        d = int(xdate)
        y = int(xyear) if xyear else 26
        year = 2000 + y if y < 100 else y
        return datetime(year, m, d).date()
    except (ValueError, TypeError):
        return datetime(2099, 12, 31).date()


@app.get("/api/flow")
async def query_flow(
    ticker: str,
    days: int = Query(0, ge=0),
    source: str = Query(None),
    side: str = Query("both"),
    sort: str = Query("dollar"),
    min_dollar: float = Query(0, ge=0),
    min_qty: float = Query(0, ge=0),
    view: str = Query("orders"),
):
    ticker = ticker.upper()
    conn = get_connection()

    # Build query
    conditions = ["ticker = ?"]
    params = [ticker]

    if source:
        conditions.append("source = ?")
        params.append(source)
    if side != "both":
        conditions.append("side = ?")
        params.append(side.upper())

    where = " AND ".join(conditions)
    rows = conn.execute(f"""
        SELECT source, side, order_date, order_time, ticker, xmonth, xdate, xyear,
               dte, strike, trade_price, target_price,
               call_qty, call_dollar, put_qty, put_dollar, insights, direction
        FROM flow_orders
        WHERE {where}
    """, params).fetchall()
    conn.close()

    total_count = len(rows)

    # Filter & build entries
    cutoff = None
    if days > 0:
        cutoff = datetime.now().date() - timedelta(days=days)

    entries = []
    for r in rows:
        order_date = _parse_order_date(r["order_date"]) if r["order_date"] else None
        if cutoff and order_date and order_date < cutoff:
            continue

        total_dollar = (r["call_dollar"] or 0) + (r["put_dollar"] or 0)
        total_qty = (r["call_qty"] or 0) + (r["put_qty"] or 0)

        if min_dollar > 0 and total_dollar < min_dollar:
            continue
        if min_qty > 0 and total_qty < min_qty:
            continue

        entries.append({
            "source": r["source"] or "",
            "side": r["side"] or "",
            "order_date": r["order_date"] or "",
            "order_time": r["order_time"] or "",
            "xmonth": r["xmonth"] or "",
            "xdate": r["xdate"] or "",
            "xyear": r["xyear"] or "",
            "dte": r["dte"] or "",
            "strike": r["strike"] or "",
            "trade_price": r["trade_price"] or "",
            "target_price": r["target_price"] or "",
            "call_qty": r["call_qty"] or 0,
            "call_dollar": r["call_dollar"] or 0,
            "put_qty": r["put_qty"] or 0,
            "put_dollar": r["put_dollar"] or 0,
            "total_dollar": total_dollar,
            "total_qty": total_qty,
            "insights": r["insights"] or "",
            "direction": r["direction"] or "",
        })

    # Sort
    if sort == "expiry":
        entries.sort(key=lambda e: _parse_expiry(e["xmonth"], e["xdate"], e["xyear"]))
    elif sort == "qty":
        entries.sort(key=lambda e: e["total_qty"], reverse=True)
    else:
        entries.sort(key=lambda e: e["total_dollar"], reverse=True)

    # Summary
    bull_entries = [e for e in entries if e["direction"] == "BULLISH"]
    bear_entries = [e for e in entries if e["direction"] == "BEARISH"]
    bull_dollar = sum(e["total_dollar"] for e in bull_entries)
    bear_dollar = sum(e["total_dollar"] for e in bear_entries)
    net_dollar = bull_dollar - bear_dollar
    direction = "BULLISH" if bull_dollar > bear_dollar else ("BEARISH" if bear_dollar > bull_dollar else "NEUTRAL")

    summary = {
        "bullish_dollar": bull_dollar,
        "bearish_dollar": bear_dollar,
        "bullish_count": len(bull_entries),
        "bearish_count": len(bear_entries),
        "bullish_qty": sum(e["total_qty"] for e in bull_entries),
        "bearish_qty": sum(e["total_qty"] for e in bear_entries),
        "net_dollar": net_dollar,
        "direction": direction,
        "sources": sorted(set(e["source"] for e in entries)),
    }

    result = {
        "ticker": ticker,
        "total_count": total_count,
        "filtered_count": len(entries),
        "summary": summary,
    }

    if view == "by_expiry":
        result["expiries"] = _group_by_expiry(entries, sort)
    elif view == "by_source":
        result["sources_breakdown"] = _group_by_source(entries)
    else:
        result["orders"] = entries[:200]

    return result


def _group_by_expiry(entries, sort_mode):
    """Group entries by expiry date."""
    expiry_map = {}
    for e in entries:
        key = f"{e['xmonth']}/{e['xdate']}"
        if e["xyear"]:
            key += f"/{e['xyear']}"
        if not e["xmonth"] or not e["xdate"]:
            key = "(no expiry)"

        if key not in expiry_map:
            expiry_map[key] = {
                "bullish_dollar": 0, "bearish_dollar": 0,
                "bullish_qty": 0, "bearish_qty": 0,
                "bullish_count": 0, "bearish_count": 0,
            }
        m = expiry_map[key]
        if e["direction"] == "BULLISH":
            m["bullish_dollar"] += e["total_dollar"]
            m["bullish_qty"] += e["total_qty"]
            m["bullish_count"] += 1
        elif e["direction"] == "BEARISH":
            m["bearish_dollar"] += e["total_dollar"]
            m["bearish_qty"] += e["total_qty"]
            m["bearish_count"] += 1

    result = []
    for exp, m in expiry_map.items():
        net = m["bullish_dollar"] - m["bearish_dollar"]
        d = "BULLISH" if m["bullish_dollar"] > m["bearish_dollar"] else (
            "BEARISH" if m["bearish_dollar"] > m["bullish_dollar"] else "NEUTRAL")
        result.append({"expiry": exp, "net_dollar": net, "direction": d, **m})

    if sort_mode == "expiry":
        result.sort(key=lambda e: _parse_expiry(
            e["expiry"].split("/")[0] if "/" in e["expiry"] else "",
            e["expiry"].split("/")[1] if "/" in e["expiry"] else "",
            e["expiry"].split("/")[2] if e["expiry"].count("/") >= 2 else "",
        ))
    elif sort_mode == "qty":
        result.sort(key=lambda e: e["bullish_qty"] + e["bearish_qty"], reverse=True)
    else:
        result.sort(key=lambda e: abs(e["net_dollar"]), reverse=True)

    return result


def _group_by_source(entries):
    """Group entries by source."""
    source_map = {}
    for e in entries:
        src = e["source"]
        if src not in source_map:
            source_map[src] = {
                "bullish_dollar": 0, "bearish_dollar": 0,
                "bullish_qty": 0, "bearish_qty": 0,
                "bullish_count": 0, "bearish_count": 0,
            }
        m = source_map[src]
        if e["direction"] == "BULLISH":
            m["bullish_dollar"] += e["total_dollar"]
            m["bullish_qty"] += e["total_qty"]
            m["bullish_count"] += 1
        elif e["direction"] == "BEARISH":
            m["bearish_dollar"] += e["total_dollar"]
            m["bearish_qty"] += e["total_qty"]
            m["bearish_count"] += 1

    result = []
    for src in sorted(source_map.keys()):
        m = source_map[src]
        net = m["bullish_dollar"] - m["bearish_dollar"]
        d = "BULLISH" if m["bullish_dollar"] > m["bearish_dollar"] else (
            "BEARISH" if m["bearish_dollar"] > m["bullish_dollar"] else "NEUTRAL")
        result.append({"source": src, "net_dollar": net, "direction": d, **m})

    return result


# ---------------------------------------------------------------------------
# Serve frontend
# ---------------------------------------------------------------------------

@app.get("/")
async def serve_index():
    return FileResponse(
        os.path.join(os.path.dirname(os.path.abspath(__file__)), "index.html"),
        media_type="text/html",
    )
