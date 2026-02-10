#!/usr/bin/env python3
"""
Tool: Process Detailed Option Flow
Description: Processes option data with strike-level detail for interactive analysis
"""

import json
import pandas as pd
import argparse
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from dotenv import load_dotenv

load_dotenv()

def parse_date(date_str):
    """Parse date string in various formats"""
    if not date_str or pd.isna(date_str):
        return None

    date_formats = [
        '%m/%d/%y', '%m/%d/%Y', '%Y-%m-%d', '%m-%d-%Y',
        '%m-%d-%y', '%d/%m/%Y', '%d/%m/%y', '%Y/%m/%d'
    ]

    for fmt in date_formats:
        try:
            return datetime.strptime(str(date_str).strip(), fmt)
        except ValueError:
            continue
    return None

def classify_sentiment(insight_text, order_type):
    """
    Classify order as bullish, bearish, or neutral based on Order Insights

    Buying side:
    - Call + Bullish = Bullish
    - Put + Bearish = Bearish

    Selling side:
    - Call + Bearish = Bearish (selling calls)
    - Put + Bullish = Bullish (selling puts)
    """
    if pd.isna(insight_text):
        return 'Unknown'

    insight_str = str(insight_text).lower()

    # Check for call vs put
    is_call = 'call' in insight_str
    is_put = 'put' in insight_str

    # Check for bullish vs bearish
    is_bullish_signal = 'bullish' in insight_str
    is_bearish_signal = 'bearish' in insight_str

    if order_type == 'buying':
        # Buying calls = bullish, buying puts = bearish
        if is_call and is_bullish_signal:
            return 'Bullish'
        elif is_put and is_bearish_signal:
            return 'Bearish'
    elif order_type == 'selling':
        # Selling calls = bearish, selling puts = bullish
        if is_call and is_bearish_signal:
            return 'Bearish'
        elif is_put and is_bullish_signal:
            return 'Bullish'

    return 'Neutral'

def process_range_data(data, range_type, header_row_idx=0):
    """Convert raw sheet data to DataFrame with proper headers"""
    if not data or len(data) == 0:
        return pd.DataFrame()

    headers = data[header_row_idx]
    rows = data[header_row_idx + 1:]
    df = pd.DataFrame(rows)

    if len(df.columns) < len(headers):
        headers = headers[:len(df.columns)]
    df.columns = headers[:len(df.columns)]
    df['order_type'] = range_type

    # Normalize column names to handle inconsistent spacing
    # (Buying has "Put \n$" with 1 space, Selling has "Put  \n$" with 2 spaces)
    df.columns = [col.replace('  \n', ' \n') if '\n' in col else col for col in df.columns]

    # Extract Order Insights column
    insights_col = next((c for c in df.columns if 'Order Insights' in c or 'Insights' in c), None)
    if insights_col:
        df['order_insights'] = df[insights_col]
    else:
        df['order_insights'] = None

    return df

def filter_near_term_expiries(df, expiry_col, months_ahead=2):
    """Filter for expiries within next N months"""
    today = datetime.now()
    cutoff = today + relativedelta(months=months_ahead)

    def parse_expiry(exp_str):
        if pd.isna(exp_str):
            return None
        try:
            # Try parsing M/D/YYYY format
            parts = str(exp_str).split('/')
            if len(parts) == 3:
                month, day, year = parts
                if len(year) == 2:
                    year = '20' + year
                return datetime(int(year), int(month), int(day))
        except:
            pass
        return None

    df['expiry_date'] = df[expiry_col].apply(parse_expiry)
    df = df[df['expiry_date'].notna()]
    df = df[df['expiry_date'] <= cutoff]

    return df

def process_detailed_flow(days_back=15, near_term_only=True):
    """
    Process option flow with strike-level detail

    Returns:
        DataFrame with individual orders (strike level detail)
    """
    # Load raw data
    with open('.tmp/raw_sheet_data.json', 'r') as f:
        raw_data = json.load(f)

    # Process ranges
    buying_df = process_range_data(raw_data['buying'], 'buying')
    selling_df = process_range_data(raw_data['selling'], 'selling')

    print(f"Buying: {len(buying_df)} rows")
    print(f"Selling: {len(selling_df)} rows")

    # Combine
    df = pd.concat([buying_df, selling_df], ignore_index=True)
    print(f"Combined: {len(df)} rows")

    # Filter by date (using business days to exclude weekends)
    from pandas.tseries.offsets import BDay
    cutoff_date = pd.Timestamp.now() - BDay(days_back)
    cutoff_date = cutoff_date.to_pydatetime()
    print(f"\nFiltering for orders after: {cutoff_date.strftime('%Y-%m-%d')} ({days_back} business days back)")

    date_col = 'Order Date'
    if date_col in df.columns:
        df['parsed_date'] = df[date_col].apply(parse_date)
        df = df[df['parsed_date'].notna()]
        initial = len(df)
        df = df[df['parsed_date'] >= cutoff_date]
        print(f"Filtered from {initial} to {len(df)} rows")

    # Identify columns
    ticker_col = 'Ticker'
    strike_col = 'Strike'

    # Create expiry from components
    if all(c in df.columns for c in ['xMonth', 'xDate', 'xYear']):
        df['expiry'] = df['xMonth'].astype(str) + '/' + df['xDate'].astype(str) + '/' + df['xYear'].astype(str)
        expiry_col = 'expiry'

    # Filter for near-term expiries (this month + next month)
    if near_term_only:
        print(f"\nFiltering for near-term expiries (next 2 months)...")
        initial = len(df)
        df = filter_near_term_expiries(df, expiry_col, months_ahead=2)
        print(f"Filtered from {initial} to {len(df)} rows")

    # Get call/put columns
    call_qty_col = next((c for c in df.columns if 'call' in c.lower() and 'qty' in c.lower()), None)
    put_qty_col = next((c for c in df.columns if 'put' in c.lower() and 'qty' in c.lower()), None)
    call_dollar_col = next((c for c in df.columns if 'call' in c.lower() and '$' in c), None)
    put_dollar_col = next((c for c in df.columns if 'put' in c.lower() and '$' in c), None)

    # Clean numeric columns
    if strike_col in df.columns:
        df[strike_col] = pd.to_numeric(df[strike_col], errors='coerce')

    if call_qty_col:
        df['call_qty'] = pd.to_numeric(df[call_qty_col].astype(str).str.replace(',', ''), errors='coerce').fillna(0)
    else:
        df['call_qty'] = 0

    if put_qty_col:
        df['put_qty'] = pd.to_numeric(df[put_qty_col].astype(str).str.replace(',', ''), errors='coerce').fillna(0)
    else:
        df['put_qty'] = 0

    if call_dollar_col:
        df['call_dollar'] = pd.to_numeric(df[call_dollar_col].astype(str).str.replace('$', '').str.replace(',', ''), errors='coerce').fillna(0)
    else:
        df['call_dollar'] = 0

    if put_dollar_col:
        df['put_dollar'] = pd.to_numeric(df[put_dollar_col].astype(str).str.replace('$', '').str.replace(',', ''), errors='coerce').fillna(0)
    else:
        df['put_dollar'] = 0

    # Calculate totals
    df['total_dollar'] = df['call_dollar'] + df['put_dollar']
    df['total_qty'] = df['call_qty'] + df['put_qty']

    # Determine option type
    df['option_type'] = 'Unknown'
    df.loc[df['call_qty'] > 0, 'option_type'] = 'Call'
    df.loc[df['put_qty'] > 0, 'option_type'] = 'Put'

    # Classify sentiment (bullish/bearish)
    if 'order_insights' in df.columns:
        df['sentiment'] = df.apply(lambda row: classify_sentiment(row.get('order_insights'), row.get('order_type')), axis=1)
    else:
        df['sentiment'] = 'Unknown'

    # Keep only relevant columns
    keep_cols = [
        ticker_col, expiry_col, 'expiry_date', strike_col, 'parsed_date',
        'call_qty', 'put_qty', 'call_dollar', 'put_dollar',
        'total_dollar', 'total_qty', 'option_type', 'order_type', 'sentiment'
    ]

    # Add DTE if available
    if 'DTE' in df.columns:
        keep_cols.append('DTE')

    # Add order insights if available
    if 'order_insights' in df.columns:
        keep_cols.append('order_insights')

    available_cols = [c for c in keep_cols if c in df.columns]
    df = df[available_cols]

    # Rename for consistency
    df = df.rename(columns={
        ticker_col: 'ticker',
        expiry_col: 'expiry',
        strike_col: 'strike'
    })

    # Filter out invalid rows
    df = df[df['ticker'].notna()]
    df = df[df['strike'].notna()]

    return df

def identify_repeated_flows(df):
    """Identify strikes/expiries that were hit multiple times"""
    # Group by ticker, expiry, strike, and option type
    grouped = df.groupby(['ticker', 'expiry', 'strike', 'option_type']).size().reset_index(name='hit_count')

    # Mark repeated flows (hit more than once)
    repeated = grouped[grouped['hit_count'] > 1].copy()
    repeated = repeated.sort_values('hit_count', ascending=False)

    return repeated

def main():
    """Main execution"""
    # Parse command-line arguments
    parser = argparse.ArgumentParser(description='Process detailed option flow data')
    parser.add_argument('--days', type=int, default=15,
                       help='Number of business days back to analyze, excluding weekends (default: 15)')
    args = parser.parse_args()

    days_back = args.days

    print("=== Processing Detailed Option Flow ===\n")
    print(f"Analyzing past {days_back} business days")

    # Process with strike-level detail (2 months for main dashboard)
    df = process_detailed_flow(days_back=days_back, near_term_only=True)

    # Save detailed data
    output_path = '.tmp/detailed_flow.csv'
    df.to_csv(output_path, index=False)

    print(f"\n✓ Detailed flow data saved to {output_path}")
    print(f"  Total orders: {len(df)}")
    print(f"  Unique tickers: {df['ticker'].nunique()}")
    print(f"  Unique expiries: {df['expiry'].nunique()}")

    # Also save extended data (5 months) for large orders analysis
    print("\n=== Processing Extended Expiry Data (5 months) ===")
    df_extended = process_detailed_flow(days_back=days_back, near_term_only=False)

    # Filter for 5 months instead of default 2
    if 'expiry_date' in df_extended.columns:
        from datetime import datetime
        today = datetime.now()
        cutoff_5m = today + relativedelta(months=5)
        initial_ext = len(df_extended)
        df_extended = df_extended[df_extended['expiry_date'] <= cutoff_5m]
        print(f"Filtered to 5-month expiries: {initial_ext} -> {len(df_extended)} rows")

    extended_path = '.tmp/detailed_flow_extended.csv'
    df_extended.to_csv(extended_path, index=False)
    print(f"✓ Extended flow data saved to {extended_path}")

    # Identify repeated flows
    print("\n=== Identifying Repeated Flows ===")
    repeated = identify_repeated_flows(df)

    if len(repeated) > 0:
        print(f"Found {len(repeated)} strike/expiry combinations hit multiple times")
        print("\nTop 10 most repeated:")
        print(repeated.head(10).to_string(index=False))

        # Save repeated flows
        repeated_path = '.tmp/repeated_flows.csv'
        repeated.to_csv(repeated_path, index=False)
        print(f"\n✓ Repeated flows saved to {repeated_path}")
    else:
        print("No repeated flows found")

    # Summary by ticker
    print("\n=== Top 10 Tickers by Total Flow ===")
    ticker_summary = df.groupby('ticker').agg({
        'total_dollar': 'sum',
        'total_qty': 'sum',
        'call_dollar': 'sum',
        'put_dollar': 'sum'
    }).sort_values('total_dollar', ascending=False).head(10)

    print(ticker_summary.to_string())

    # Sentiment breakdown
    print("\n=== Sentiment Breakdown ===")
    if df.empty:
        print("No data to summarize.")
    else:
        sentiment_summary = df.groupby('sentiment').agg({
            'total_dollar': 'sum',
            'total_qty': 'sum'
        }).sort_values('total_dollar', ascending=False)

        print(sentiment_summary.to_string())
        print(f"\nBullish flow: ${df[df['sentiment'].str.contains('Bullish', na=False)]['total_dollar'].sum():,.0f}")
        print(f"Bearish flow: ${df[df['sentiment'].str.contains('Bearish', na=False)]['total_dollar'].sum():,.0f}")

    return df

if __name__ == "__main__":
    # Note: requires python-dateutil
    try:
        main()
    except ImportError as e:
        print("Missing dependency. Install with: pip install python-dateutil")
        raise
