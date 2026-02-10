#!/usr/bin/env python3
"""
Shared utilities for option flow monitors.

Provides common helpers for parsing, column detection, deduplication,
and message formatting used by both 7DTE and Floor monitors.
"""

import os
import re
import json
import hashlib
from datetime import date


# ---------------------------------------------------------------------------
# Parsing helpers
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
# Formatting helpers
# ---------------------------------------------------------------------------

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


# ---------------------------------------------------------------------------
# Deduplication — only alert on NEW entries since last run
# ---------------------------------------------------------------------------

STATE_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), ".tmp")


def _default_alert_key(alert):
    """Create a unique key for an alert based on its content."""
    raw = (f"{alert.get('side','')}|{alert.get('label','')}|{alert.get('field','')}|"
           f"{alert.get('call_dollar',0)}|{alert.get('call_qty',0)}|"
           f"{alert.get('put_dollar',0)}|{alert.get('put_qty',0)}")
    return hashlib.md5(raw.encode()).hexdigest()


def _row_content_key(alert):
    """Create a unique key based on monitor name + full row hash."""
    raw = f"{alert.get('monitor','')}|{alert.get('row_hash','')}"
    return hashlib.md5(raw.encode()).hexdigest()


def load_state(state_file):
    """Load previously seen alert keys. Resets automatically each day."""
    filepath = os.path.join(STATE_DIR, state_file)
    if not os.path.exists(filepath):
        return {"date": str(date.today()), "seen": []}
    try:
        with open(filepath, "r") as f:
            state = json.load(f)
        if state.get("date") != str(date.today()):
            return {"date": str(date.today()), "seen": []}
        return state
    except (json.JSONDecodeError, KeyError):
        return {"date": str(date.today()), "seen": []}


def save_state(state, state_file):
    """Persist seen alert keys to disk."""
    os.makedirs(STATE_DIR, exist_ok=True)
    filepath = os.path.join(STATE_DIR, state_file)
    with open(filepath, "w") as f:
        json.dump(state, f, indent=2)


def filter_new_alerts(alerts, state_file, key_fn=None):
    """Return only alerts we haven't sent yet today, and update state."""
    if key_fn is None:
        key_fn = _default_alert_key
    state = load_state(state_file)
    seen = set(state["seen"])
    new_alerts = []
    for a in alerts:
        key = key_fn(a)
        if key not in seen:
            new_alerts.append(a)
            seen.add(key)
    state["seen"] = list(seen)
    save_state(state, state_file)
    return new_alerts
