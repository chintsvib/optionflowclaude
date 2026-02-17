#!/usr/bin/env python3
"""
Tool: Query Flow for a Ticker (Unified DB)

Reads the unified SQLite database (loaded by unified_db.py) which contains
data from allDay, 7Day, and Floor sheets. Uses Order Insights for direction.

Usage:
    python tools/query_flow.py NVDA
    python tools/query_flow.py SPY --days 7
    python tools/query_flow.py AAPL --source allDay --min-dollar 500000
    python tools/query_flow.py TSLA --source 7Day --min-qty 500
    python tools/query_flow.py QQQ --sort qty
    python tools/query_flow.py SPY --by-expiry --sort expiry
    python tools/query_flow.py NVDA --by-source
"""

import os
import sys
import argparse
from datetime import datetime, timedelta

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from rich.console import Console
from rich.table import Table
from rich.text import Text
from rich.panel import Panel

from tools.unified_db import get_connection

console = Console()


def dir_text(direction, short=False):
    """Return a colored Text object for direction."""
    label = direction
    if short:
        label = "BULL" if direction == "BULLISH" else ("BEAR" if direction == "BEARISH" else direction)
    if direction == "BULLISH":
        return Text(label, style="bold green")
    elif direction == "BEARISH":
        return Text(label, style="bold red")
    return Text(label, style="dim")


def dollar_text(val, color=None):
    """Return a colored Text for dollar value. Auto-colors if no explicit color."""
    s = fmt_dollar(val)
    if color:
        return Text(s, style=color)
    if val > 0:
        return Text(s, style="green")
    elif val < 0:
        return Text(s, style="red")
    return Text(s, style="dim")


def bar_text(bullish, bearish, width=20):
    """Return a colored bar showing bullish vs bearish ratio."""
    total = bullish + bearish
    if total == 0:
        return Text("░" * width, style="dim")
    bull_w = round((bullish / total) * width)
    bear_w = width - bull_w
    bar = Text()
    bar.append("█" * bull_w, style="green")
    bar.append("█" * bear_w, style="red")
    return bar


def parse_order_date(date_str):
    """Parse M/D/YY date string to datetime.date."""
    try:
        parts = date_str.strip().split("/")
        if len(parts) == 3:
            m, d, y = int(parts[0]), int(parts[1]), int(parts[2])
            year = 2000 + y if y < 100 else y
            return datetime(year, m, d).date()
    except (ValueError, IndexError):
        pass
    return None


def parse_expiry(xmonth, xdate, xyear):
    """Parse xmonth/xdate/xyear into a sortable date (or far future if missing)."""
    try:
        m = int(xmonth)
        d = int(xdate)
        y = int(xyear) if xyear else 26
        year = 2000 + y if y < 100 else y
        return datetime(year, m, d).date()
    except (ValueError, TypeError):
        return datetime(2099, 12, 31).date()


def fmt_dollar(val):
    """Format dollar value with K/M suffix."""
    if val is None:
        return "$0"
    abs_val = abs(val)
    sign = "" if val >= 0 else "-"
    if abs_val >= 1_000_000:
        return f"{sign}${abs_val / 1_000_000:.2f}M"
    elif abs_val >= 1_000:
        return f"{sign}${abs_val / 1_000:.1f}K"
    else:
        return f"{sign}${abs_val:.0f}"


def fmt_qty(val):
    """Format quantity with commas."""
    if val is None or val == 0:
        return "0"
    return f"{val:,.0f}"


def main():
    parser = argparse.ArgumentParser(
        description="Query flow for a ticker from unified database",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python tools/query_flow.py NVDA                       # all sources
  python tools/query_flow.py SPY --days 7                # past 7 days
  python tools/query_flow.py AAPL --source allDay        # allDay only
  python tools/query_flow.py TSLA --source 7Day          # 7Day only
  python tools/query_flow.py QQQ --sort qty              # sort by quantity
  python tools/query_flow.py SPY --by-expiry --sort expiry
  python tools/query_flow.py NVDA --by-source            # breakdown by source
  python tools/query_flow.py NVDA --side buying          # buying side only
        """,
    )
    parser.add_argument("ticker", type=str, help="Ticker symbol (e.g. NVDA, SPY)")
    parser.add_argument("--days", type=int, default=0,
                        help="Only show orders from past N days (0 = all)")
    parser.add_argument("--source", type=str, default=None,
                        help="Filter by source (allDay, 7Day, Floor_SPX_0DTE, etc.)")
    parser.add_argument("--side", choices=["buying", "selling", "both"], default="both",
                        help="Filter by side (default: both)")
    parser.add_argument("--sort", choices=["dollar", "qty", "expiry"], default="dollar",
                        help="Sort by dollar amount, quantity, or expiry date (default: dollar)")
    parser.add_argument("--min-dollar", type=float, default=0,
                        help="Minimum dollar amount to show (call$ + put$)")
    parser.add_argument("--min-qty", type=float, default=0,
                        help="Minimum quantity to show (call_qty + put_qty)")
    parser.add_argument("--by-expiry", action="store_true",
                        help="Group and summarize by expiry date")
    parser.add_argument("--by-source", action="store_true",
                        help="Show breakdown by source (allDay, 7Day, Floor)")
    args = parser.parse_args()

    ticker = args.ticker.upper()
    conn = get_connection()

    # Check if DB has data
    try:
        total = conn.execute("SELECT COUNT(*) as c FROM flow_orders").fetchone()["c"]
    except Exception:
        total = 0
    if total == 0:
        print("No data in unified database. Run: python tools/unified_db.py")
        conn.close()
        return

    # Build query
    conditions = ["ticker = ?"]
    params = [ticker]

    if args.source:
        conditions.append("source = ?")
        params.append(args.source)

    if args.side != "both":
        conditions.append("side = ?")
        params.append(args.side.upper())

    where = " AND ".join(conditions)

    rows = conn.execute(f"""
        SELECT source, side, order_date, order_time, ticker, xmonth, xdate, xyear,
               dte, strike, trade_price, target_price,
               call_qty, call_dollar, put_qty, put_dollar, insights, direction
        FROM flow_orders
        WHERE {where}
    """, params).fetchall()
    conn.close()

    if not rows:
        print(f"No entries found for {ticker}.")
        return

    # Convert to dicts and filter by days
    entries = []
    cutoff = None
    if args.days > 0:
        cutoff = datetime.now().date() - timedelta(days=args.days)

    for r in rows:
        order_date = parse_order_date(r["order_date"]) if r["order_date"] else None
        if cutoff and order_date and order_date < cutoff:
            continue

        total_dollar = (r["call_dollar"] or 0) + (r["put_dollar"] or 0)
        total_qty = (r["call_qty"] or 0) + (r["put_qty"] or 0)

        if args.min_dollar > 0 and total_dollar < args.min_dollar:
            continue
        if args.min_qty > 0 and total_qty < args.min_qty:
            continue

        entries.append({
            "source": r["source"] or "",
            "side": r["side"],
            "order_date": r["order_date"] or "",
            "order_date_parsed": order_date,
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

    if not entries:
        filters = []
        if args.days > 0:
            filters.append(f"past {args.days} days")
        if args.min_dollar > 0:
            filters.append(f">= {fmt_dollar(args.min_dollar)}")
        if args.min_qty > 0:
            filters.append(f">= {fmt_qty(args.min_qty)} qty")
        print(f"No entries for {ticker} matching filters: {', '.join(filters)}")
        return

    # Sort
    if args.sort == "expiry":
        entries.sort(key=lambda e: parse_expiry(e["xmonth"], e["xdate"], e["xyear"]))
    elif args.sort == "qty":
        entries.sort(key=lambda e: e["total_qty"], reverse=True)
    else:
        entries.sort(key=lambda e: e["total_dollar"], reverse=True)

    # --- By-source summary ---
    if args.by_source:
        _print_by_source(ticker, entries, args)
        return

    # --- By-expiry summary ---
    if args.by_expiry:
        _print_by_expiry(ticker, entries, args)
        return

    # --- Individual entries ---
    _print_entries(ticker, entries, args)


def _print_by_source(ticker, entries, args):
    """Print flow breakdown by source (allDay, 7Day, Floor sections)."""
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

    days_label = f" (past {args.days} days)" if args.days > 0 else ""

    table = Table(
        title=f"{ticker} — Flow by Source{days_label}",
        caption=f"{len(entries)} orders across {len(source_map)} sources",
        border_style="bright_blue",
        title_style="bold cyan",
        caption_style="dim",
        show_lines=True,
    )
    table.add_column("Source", style="bold white", min_width=14)
    table.add_column("Bullish$", justify="right", style="green")
    table.add_column("Bearish$", justify="right", style="red")
    table.add_column("Net$", justify="right")
    table.add_column("Flow", min_width=20)
    table.add_column("Dir", justify="center")
    table.add_column("#Bull/#Bear", justify="center", style="dim")

    total_bull = 0
    total_bear = 0
    total_bull_count = 0
    total_bear_count = 0

    for src in sorted(source_map.keys()):
        m = source_map[src]
        net = m["bullish_dollar"] - m["bearish_dollar"]
        direction = "BULLISH" if m["bullish_dollar"] > m["bearish_dollar"] else (
            "BEARISH" if m["bearish_dollar"] > m["bullish_dollar"] else "NEUTRAL")
        table.add_row(
            src,
            fmt_dollar(m["bullish_dollar"]),
            fmt_dollar(m["bearish_dollar"]),
            dollar_text(net),
            bar_text(m["bullish_dollar"], m["bearish_dollar"]),
            dir_text(direction),
            f"{m['bullish_count']}/{m['bearish_count']}",
        )
        total_bull += m["bullish_dollar"]
        total_bear += m["bearish_dollar"]
        total_bull_count += m["bullish_count"]
        total_bear_count += m["bearish_count"]

    overall_net = total_bull - total_bear
    overall_dir = "BULLISH" if total_bull > total_bear else ("BEARISH" if total_bear > total_bull else "NEUTRAL")
    table.add_row(
        Text("TOTAL", style="bold"),
        Text(fmt_dollar(total_bull), style="bold green"),
        Text(fmt_dollar(total_bear), style="bold red"),
        dollar_text(overall_net),
        bar_text(total_bull, total_bear),
        dir_text(overall_dir),
        f"{total_bull_count}/{total_bear_count}",
        style="on grey15",
    )

    console.print()
    console.print(table)


def _print_by_expiry(ticker, entries, args):
    """Print flow grouped by expiry, using Order Insights for direction."""
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

    # Build sorted list
    expiry_list = []
    for exp, m in expiry_map.items():
        net_dollar = m["bullish_dollar"] - m["bearish_dollar"]

        if m["bullish_dollar"] > m["bearish_dollar"]:
            direction = "BULLISH"
        elif m["bearish_dollar"] > m["bullish_dollar"]:
            direction = "BEARISH"
        else:
            direction = "NEUTRAL"

        expiry_list.append({
            "expiry": exp,
            "bullish_dollar": m["bullish_dollar"],
            "bearish_dollar": m["bearish_dollar"],
            "bullish_qty": m["bullish_qty"],
            "bearish_qty": m["bearish_qty"],
            "net_dollar": net_dollar,
            "bullish_count": m["bullish_count"],
            "bearish_count": m["bearish_count"],
            "direction": direction,
        })

    if args.sort == "expiry":
        expiry_list.sort(key=lambda e: parse_expiry(
            e["expiry"].split("/")[0] if "/" in e["expiry"] else "",
            e["expiry"].split("/")[1] if "/" in e["expiry"] else "",
            e["expiry"].split("/")[2] if e["expiry"].count("/") >= 2 else "",
        ))
    elif args.sort == "qty":
        expiry_list.sort(key=lambda e: e["bullish_qty"] + e["bearish_qty"], reverse=True)
    else:
        expiry_list.sort(key=lambda e: abs(e["net_dollar"]), reverse=True)

    days_label = f" (past {args.days} days)" if args.days > 0 else ""
    side_label = f" [{args.side.upper()}]" if args.side != "both" else ""

    table = Table(
        title=f"{ticker} — Flow by Expiry{days_label}{side_label}",
        caption=f"{len(entries)} orders across {len(expiry_list)} expiries",
        border_style="bright_blue",
        title_style="bold cyan",
        caption_style="dim",
        show_lines=True,
    )
    table.add_column("Expiry", style="bold white", min_width=10)
    table.add_column("Bullish$", justify="right", style="green")
    table.add_column("Bearish$", justify="right", style="red")
    table.add_column("Bull Qty", justify="right", style="green")
    table.add_column("Bear Qty", justify="right", style="red")
    table.add_column("Flow", min_width=20)
    table.add_column("Dir", justify="center")
    table.add_column("#B/#R", justify="center", style="dim")

    for e in expiry_list:
        table.add_row(
            e["expiry"],
            fmt_dollar(e["bullish_dollar"]),
            fmt_dollar(e["bearish_dollar"]),
            fmt_qty(e["bullish_qty"]),
            fmt_qty(e["bearish_qty"]),
            bar_text(e["bullish_dollar"], e["bearish_dollar"]),
            dir_text(e["direction"]),
            f"{e['bullish_count']}/{e['bearish_count']}",
        )

    # Totals
    total_bullish = sum(e["bullish_dollar"] for e in expiry_list)
    total_bearish = sum(e["bearish_dollar"] for e in expiry_list)
    total_bull_qty = sum(e["bullish_qty"] for e in expiry_list)
    total_bear_qty = sum(e["bearish_qty"] for e in expiry_list)
    total_bull_count = sum(e["bullish_count"] for e in expiry_list)
    total_bear_count = sum(e["bearish_count"] for e in expiry_list)

    overall_dir = "BULLISH" if total_bullish > total_bearish else ("BEARISH" if total_bearish > total_bullish else "NEUTRAL")

    table.add_row(
        Text("TOTAL", style="bold"),
        Text(fmt_dollar(total_bullish), style="bold green"),
        Text(fmt_dollar(total_bearish), style="bold red"),
        Text(fmt_qty(total_bull_qty), style="bold green"),
        Text(fmt_qty(total_bear_qty), style="bold red"),
        bar_text(total_bullish, total_bearish),
        dir_text(overall_dir),
        f"{total_bull_count}/{total_bear_count}",
        style="on grey15",
    )

    console.print()
    console.print(table)


def _print_entries(ticker, entries, args):
    """Print individual order entries."""
    days_label = f" (past {args.days} days)" if args.days > 0 else ""
    side_label = f" [{args.side.upper()}]" if args.side != "both" else ""
    source_label = f" [{args.source}]" if args.source else ""
    filters = []
    if args.min_dollar > 0:
        filters.append(f"min ${args.min_dollar:,.0f}")
    if args.min_qty > 0:
        filters.append(f"min {args.min_qty:,.0f} qty")
    filter_label = f" ({', '.join(filters)})" if filters else ""
    sort_label = args.sort

    # Summary panel
    bullish_entries = [e for e in entries if e["direction"] == "BULLISH"]
    bearish_entries = [e for e in entries if e["direction"] == "BEARISH"]
    bullish_dollar = sum(e["total_dollar"] for e in bullish_entries)
    bearish_dollar = sum(e["total_dollar"] for e in bearish_entries)
    net_dollar = bullish_dollar - bearish_dollar
    overall_dir = "BULLISH" if bullish_dollar > bearish_dollar else ("BEARISH" if bearish_dollar > bullish_dollar else "NEUTRAL")
    sources = sorted(set(e["source"] for e in entries))

    summary = Text()
    summary.append("Sources: ", style="dim")
    summary.append(", ".join(sources) + "\n")
    summary.append("Bullish: ", style="green bold")
    summary.append(f"{len(bullish_entries)} orders | {fmt_dollar(bullish_dollar)}\n", style="green")
    summary.append("Bearish: ", style="red bold")
    summary.append(f"{len(bearish_entries)} orders | {fmt_dollar(bearish_dollar)}\n", style="red")
    summary.append("Net:     ", style="bold")
    net_style = "bold green" if net_dollar > 0 else ("bold red" if net_dollar < 0 else "dim")
    summary.append(f"{fmt_dollar(net_dollar)} — ", style=net_style)
    summary.append(overall_dir, style=net_style)
    summary.append("  ")
    summary.append_text(bar_text(bullish_dollar, bearish_dollar, width=30))

    console.print()
    console.print(Panel(
        summary,
        title=f"{ticker} — {len(entries)} orders{days_label}{source_label}{side_label}{filter_label}",
        subtitle=f"sorted by {sort_label}",
        border_style="bright_blue",
        title_align="left",
    ))

    # Individual entries table
    table = Table(
        border_style="bright_blue",
        show_lines=False,
        pad_edge=True,
        row_styles=["", "on grey7"],
    )
    table.add_column("Dir", justify="center", min_width=4, no_wrap=True)
    table.add_column("Src", style="cyan", min_width=6, no_wrap=True)
    table.add_column("Date", style="white", min_width=8, no_wrap=True)
    table.add_column("Strike", justify="right", style="yellow", min_width=6, no_wrap=True)
    table.add_column("Exp", justify="right", min_width=5, no_wrap=True)
    table.add_column("Total$", justify="right", min_width=8, no_wrap=True)
    table.add_column("Qty", justify="right", min_width=6, no_wrap=True)
    table.add_column("Insight", no_wrap=True, overflow="ellipsis")

    for e in entries[:50]:
        exp = f"{e['xmonth']}/{e['xdate']}" if e["xmonth"] and e["xdate"] else ""
        insight_short = e["insights"][:25] if e["insights"] else ""
        table.add_row(
            dir_text(e["direction"], short=True),
            e["source"][:8],
            e["order_date"],
            e["strike"],
            exp,
            dollar_text(e["total_dollar"], "green" if e["direction"] == "BULLISH" else "red"),
            fmt_qty(e["total_qty"]),
            Text(insight_short, style="dim"),
        )

    console.print(table)
    if len(entries) > 50:
        console.print(f"  [dim]... and {len(entries) - 50} more entries (showing top 50)[/dim]")
    console.print()


if __name__ == "__main__":
    main()
