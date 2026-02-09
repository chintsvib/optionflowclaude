#!/usr/bin/env python3
"""
Tool: Fetch EMA Status
Description: Checks if stock price is above 39 EMA across multiple timeframes
"""

import pandas as pd
import yfinance as yf
from datetime import datetime
import warnings
warnings.filterwarnings('ignore')

def calculate_ema(prices, period=39):
    """Calculate Exponential Moving Average"""
    return prices.ewm(span=period, adjust=False).mean()

def fetch_ema_status_for_ticker(ticker):
    """
    Fetch price and EMA status for a single ticker across multiple timeframes

    Returns dict with:
    - ticker
    - current_price
    - timeframes (5m, 10m, 1h, 4h, 1d, 1wk)
    - ema_values
    - above_ema (boolean)
    - distance_pct
    """
    try:
        stock = yf.Ticker(ticker)

        # Define timeframes and their yfinance intervals
        timeframes = {
            '5m': ('5m', '5d'),      # 5 min data for past 5 days
            '10m': ('15m', '5d'),    # Use 15m as proxy, yfinance doesn't have 10m
            '1h': ('1h', '1mo'),     # 1 hour data for past month
            '4h': ('1h', '3mo'),     # Use 1h data, calculate 4h manually
            '1d': ('1d', '6mo'),     # Daily data for 6 months
            '1wk': ('1wk', '2y')     # Weekly data for 2 years
        }

        results = {
            'ticker': ticker,
            'current_price': None,
            'timeframes': {}
        }

        # Get current price from most recent 5m data
        try:
            recent_data = stock.history(period='1d', interval='5m')
            if len(recent_data) > 0:
                results['current_price'] = recent_data['Close'].iloc[-1]
        except:
            # Fallback to daily data
            recent_data = stock.history(period='1d', interval='1d')
            if len(recent_data) > 0:
                results['current_price'] = recent_data['Close'].iloc[-1]

        if results['current_price'] is None:
            print(f"⚠️  {ticker}: Could not fetch current price")
            return results

        # Check each timeframe
        for tf_name, (interval, period) in timeframes.items():
            try:
                hist = stock.history(period=period, interval=interval)

                if len(hist) < 39:
                    results['timeframes'][tf_name] = {
                        'ema': None,
                        'above': None,
                        'distance_pct': None,
                        'error': 'Insufficient data'
                    }
                    continue

                # For 4h, resample 1h data
                if tf_name == '4h' and interval == '1h':
                    hist = hist.resample('4h').agg({
                        'Open': 'first',
                        'High': 'max',
                        'Low': 'min',
                        'Close': 'last',
                        'Volume': 'sum'
                    }).dropna()

                # For 10m, use 15m as approximation
                if tf_name == '10m':
                    tf_name_display = '10m*'  # Indicate approximation
                else:
                    tf_name_display = tf_name

                # Calculate 39 EMA
                ema_values = calculate_ema(hist['Close'], period=39)
                latest_ema = ema_values.iloc[-1]

                # Compare current price to EMA
                above_ema = results['current_price'] > latest_ema
                distance_pct = ((results['current_price'] - latest_ema) / latest_ema) * 100

                results['timeframes'][tf_name] = {
                    'ema': latest_ema,
                    'above': above_ema,
                    'distance_pct': distance_pct
                }

            except Exception as e:
                results['timeframes'][tf_name] = {
                    'ema': None,
                    'above': None,
                    'distance_pct': None,
                    'error': str(e)
                }

        return results

    except Exception as e:
        print(f"❌ {ticker}: Error - {e}")
        return {
            'ticker': ticker,
            'current_price': None,
            'timeframes': {},
            'error': str(e)
        }

def format_ema_status(results):
    """Format EMA status for display"""
    ticker = results['ticker']
    price = results['current_price']

    if price is None:
        return f"{ticker}: No data available"

    lines = [f"\n{ticker}: ${price:.2f}"]

    tf_order = ['5m', '10m', '1h', '4h', '1d', '1wk']
    bullish_count = 0
    total_count = 0

    for tf in tf_order:
        if tf in results['timeframes']:
            tf_data = results['timeframes'][tf]

            if tf_data.get('error'):
                lines.append(f"  ⚠️  {tf:4s} - {tf_data['error']}")
                continue

            if tf_data['ema'] is None:
                continue

            total_count += 1
            icon = "✅" if tf_data['above'] else "❌"
            if tf_data['above']:
                bullish_count += 1

            ema = tf_data['ema']
            dist = tf_data['distance_pct']

            lines.append(f"  {icon} {tf:4s} EMA: ${ema:>8.2f}  ({dist:+.2f}%)")

    if total_count > 0:
        lines.append(f"\n  Score: {bullish_count}/{total_count} timeframes bullish")

    return '\n'.join(lines)

def main():
    """Main execution"""
    print("=== Fetching EMA Status Across Timeframes ===\n")
    print("Checking if price is above 39 EMA for: 5M, 10M, 1H, 4H, Daily, Weekly\n")

    # Focus tickers from your dashboard
    focus_tickers = [
        'NVDA', 'GOOG', 'GOOGL', 'AAPL', 'MSFT', 'AMZN', 'META', 'AVGO',
        'TSM', 'TSLA', 'SPY', 'QQQ', 'SPX', 'NDX', 'PLTR', 'AMD', 'UBER',
        'QCOM', 'GLD', 'SLV', 'MU', 'NOW'
    ]

    all_results = []

    for ticker in focus_tickers:
        print(f"Fetching {ticker}...")
        results = fetch_ema_status_for_ticker(ticker)
        all_results.append(results)

        # Print formatted status
        print(format_ema_status(results))

    # Convert to DataFrame for saving
    rows = []
    for result in all_results:
        if result['current_price'] is None:
            continue

        row = {
            'ticker': result['ticker'],
            'current_price': result['current_price']
        }

        # Add each timeframe
        for tf in ['5m', '10m', '1h', '4h', '1d', '1wk']:
            if tf in result['timeframes']:
                tf_data = result['timeframes'][tf]
                row[f'{tf}_ema'] = tf_data.get('ema')
                row[f'{tf}_above'] = tf_data.get('above')
                row[f'{tf}_distance_pct'] = tf_data.get('distance_pct')
            else:
                row[f'{tf}_ema'] = None
                row[f'{tf}_above'] = None
                row[f'{tf}_distance_pct'] = None

        # Calculate bullish count
        above_counts = [row[f'{tf}_above'] for tf in ['5m', '10m', '1h', '4h', '1d', '1wk']
                       if row[f'{tf}_above'] is not None]
        row['bullish_count'] = sum(above_counts)
        row['total_timeframes'] = len(above_counts)

        rows.append(row)

    df = pd.DataFrame(rows)

    # Save to CSV
    output_path = '.tmp/ema_status.csv'
    df.to_csv(output_path, index=False)

    print(f"\n✓ EMA status saved to {output_path}")

    # Summary
    print("\n=== Summary ===")
    print(f"Total tickers analyzed: {len(df)}")

    # Show tickers with all timeframes bullish
    all_bullish = df[df['bullish_count'] == df['total_timeframes']]
    if len(all_bullish) > 0:
        print(f"\n🚀 Tickers with ALL timeframes above 39 EMA:")
        for ticker in all_bullish['ticker'].tolist():
            print(f"   {ticker}")

    # Show tickers with 0 timeframes bullish
    all_bearish = df[df['bullish_count'] == 0]
    if len(all_bearish) > 0:
        print(f"\n📉 Tickers with NO timeframes above 39 EMA:")
        for ticker in all_bearish['ticker'].tolist():
            print(f"   {ticker}")

    return df

if __name__ == "__main__":
    try:
        main()
    except ImportError as e:
        print("Missing dependency. Install with: pip install yfinance")
        raise
