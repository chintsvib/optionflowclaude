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


def send_telegram(message, bot_token=None, chat_id=None, parse_mode="HTML"):
    """
    Send a message via Telegram Bot API.

    Args:
        message: Text to send (supports HTML formatting)
        bot_token: Telegram bot token (defaults to TELEGRAM_BOT_TOKEN env var)
        chat_id: Telegram chat ID (defaults to TELEGRAM_CHAT_ID env var)
        parse_mode: "HTML" or "Markdown"

    Returns:
        True if sent successfully, False otherwise
    """
    bot_token = bot_token or os.getenv("TELEGRAM_BOT_TOKEN")
    chat_id = chat_id or os.getenv("TELEGRAM_CHAT_ID")

    if not bot_token:
        print("Error: TELEGRAM_BOT_TOKEN not set in .env")
        return False
    if not chat_id:
        print("Error: TELEGRAM_CHAT_ID not set in .env")
        return False

    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    payload = json.dumps({
        "chat_id": chat_id,
        "text": message,
        "parse_mode": parse_mode,
    }).encode("utf-8")

    req = urllib.request.Request(url, data=payload, headers={"Content-Type": "application/json"})

    try:
        with urllib.request.urlopen(req) as resp:
            result = json.loads(resp.read().decode())
            if result.get("ok"):
                print("Telegram message sent successfully.")
                return True
            else:
                print(f"Telegram API error: {result}")
                return False
    except urllib.error.HTTPError as e:
        body = e.read().decode()
        print(f"Telegram HTTP error {e.code}: {body}")
        return False
    except Exception as e:
        print(f"Failed to send Telegram message: {e}")
        return False


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python tools/send_telegram.py \"Your message here\"")
        sys.exit(1)

    msg = " ".join(sys.argv[1:])
    success = send_telegram(msg)
    sys.exit(0 if success else 1)
