#!/usr/bin/env python3
"""
Tool: Send Telegram Message
Description: Sends a message via Telegram Bot API (no extra dependencies, uses urllib)
Usage: Can be imported or run standalone:
    python tools/send_telegram.py "Your message here"
"""

import os
import sys
import json
import urllib.request
import urllib.parse
import urllib.error
from dotenv import load_dotenv

load_dotenv()


def _send_one(message, bot_token, chat_id, parse_mode):
    """Send a message to a single chat_id. Returns True on success."""
    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    payload = json.dumps({
        "chat_id": chat_id.strip(),
        "text": message,
        "parse_mode": parse_mode,
    }).encode("utf-8")

    req = urllib.request.Request(url, data=payload, headers={"Content-Type": "application/json"})

    try:
        with urllib.request.urlopen(req) as resp:
            result = json.loads(resp.read().decode())
            if result.get("ok"):
                print(f"Telegram message sent to {chat_id.strip()}.")
                return True
            else:
                print(f"Telegram API error for {chat_id.strip()}: {result}")
                return False
    except urllib.error.HTTPError as e:
        body = e.read().decode()
        print(f"Telegram HTTP error {e.code} for {chat_id.strip()}: {body}")
        return False
    except Exception as e:
        print(f"Failed to send Telegram message to {chat_id.strip()}: {e}")
        return False


def send_telegram(message, bot_token=None, chat_id=None, parse_mode="HTML"):
    """
    Send a message via Telegram Bot API to one or more chat IDs.

    Args:
        message: Text to send (supports HTML formatting)
        bot_token: Telegram bot token (defaults to TELEGRAM_BOT_TOKEN env var)
        chat_id: Comma-separated chat IDs (defaults to TELEGRAM_CHAT_ID env var)
        parse_mode: "HTML" or "Markdown"

    Returns:
        True if sent to all recipients successfully, False otherwise
    """
    bot_token = bot_token or os.getenv("TELEGRAM_BOT_TOKEN")
    chat_ids_raw = chat_id or os.getenv("TELEGRAM_CHAT_ID")

    if not bot_token:
        print("Error: TELEGRAM_BOT_TOKEN not set in .env")
        return False
    if not chat_ids_raw:
        print("Error: TELEGRAM_CHAT_ID not set in .env")
        return False

    chat_ids = [c.strip() for c in chat_ids_raw.split(",") if c.strip()]
    all_ok = True
    for cid in chat_ids:
        if not _send_one(message, bot_token, cid, parse_mode):
            all_ok = False
    return all_ok


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python tools/send_telegram.py \"Your message here\"")
        sys.exit(1)

    msg = " ".join(sys.argv[1:])
    success = send_telegram(msg)
    sys.exit(0 if success else 1)
