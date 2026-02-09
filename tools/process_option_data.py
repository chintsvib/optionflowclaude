#!/usr/bin/env python3
"""
Tool: Process Option Data
Description: Filters, aggregates, and analyzes option flow data
"""

import json
import pandas as pd
from datetime import datetime, timedelta
from dotenv import load_dotenv

load_dotenv()

def parse_date(date_str):
    """Parse date string in various formats"""
    if not date_str or pd.isna(date_str):
        return None

    date_formats = [
        '%m/%d/%y',      # 1/7/26
        '%m/%d/%Y',      # 1/7/2026
        '%Y-%m-%d',      # 2026-01-07
        '%m-%d-%Y',      # 01-07-2026
        '%m-%d-%y',      # 01-07-26
        '%d/%m/%Y',      # 07/01/2026
        '%d/%m/%y',      # 07/01/26
        '%Y/%m/%d'       # 2026/01/07
    ]

    for fmt in date_formats:
        try:
            return datetime.strptime(str(date_str).strip(), fmt)
        except ValueError:
            continue

    # Don't print warning for every failed date
    return None

def process_range_data(data, range_type, header_row_idx=0):
    """
    Convert raw sheet data to DataFrame with proper headers

    Args:
        data: List of lists from Google Sheets
        range_type: 'buying' or 'selling'
        header_row_idx: Index of header row (usually 0 since we start from row 3)

    Returns:
        DataFrame with processed data
    """
    if not data or len(data) == 0:
        return pd.DataFrame()

    # First row is headers
    headers = data[header_row_idx]
    rows = data[header_row_idx + 1:]

    # Create DataFrame
    df = pd.DataFrame(rows)

    # Set column names, handling case where rows have fewer columns than headers
    if len(df.columns) < len(headers):
        headers = headers[:len(df.columns)]
    df.columns = headers[:len(df.columns)]

    # Add range type
    df['order_type'] = range_type

    return df

def aggregate_option_flow(df, days_back=15):
    """
    Filter and aggregate option flow data

    Args:
        df: Combined DataFrame with all option data
        days_back: Only include orders from past N days

    Returns:
        Aggregated DataFrame grouped by ticker and expiry
    """
    # Calculate cutoff date
    cutoff_date = datetime.now() - timedelta(days=days_back)
    print(f"\nFiltering for orders after: {cutoff_date.strftime('%Y-%m-%d')}")

    # Try to identify date column (common names)
    date_columns = ['Order Date', 'Date', 'Trade Date', 'date', 'trade_date', 'Time', 'Timestamp']
    date_col = None
    for col in date_columns:
        if col in df.columns:
            date_col = col
            break

    if not date_col:
        print(f"Warning: Could not find date column. Available columns: {df.columns.tolist()}")
        print("Using all data without date filtering")
    else:
        print(f"Using date column: {date_col}")
        # Parse dates
        df['parsed_date'] = df[date_col].apply(parse_date)

        # Filter by date
        df = df[df['parsed_date'].notna()]
        initial_count = len(df)
        df = df[df['parsed_date'] >= cutoff_date]
        print(f"Filtered from {initial_count} to {len(df)} rows")

    # Identify ticker column
    ticker_cols = ['Ticker', 'Symbol', 'ticker', 'symbol', 'Stock']
    ticker_col = next((c for c in ticker_cols if c in df.columns), None)

    # Create expiry from xMonth, xDate, xYear if available
    expiry_col = None
    if all(c in df.columns for c in ['xMonth', 'xDate', 'xYear']):
        df['expiry'] = df['xMonth'].astype(str) + '/' + df['xDate'].astype(str) + '/' + df['xYear'].astype(str)
        expiry_col = 'expiry'
    else:
        expiry_cols = ['Expiry', 'Expiration', 'Exp Date', 'expiry', 'expiration']
        expiry_col = next((c for c in expiry_cols if c in df.columns), None)

    # Handle Call/Put quantities and dollars with actual column names
    call_qty_col = next((c for c in df.columns if 'call' in c.lower() and 'qty' in c.lower()), None)
    put_qty_col = next((c for c in df.columns if 'put' in c.lower() and 'qty' in c.lower()), None)
    call_dollar_col = next((c for c in df.columns if 'call' in c.lower() and '$' in c), None)
    put_dollar_col = next((c for c in df.columns if 'put' in c.lower() and '$' in c), None)

    print(f"\nIdentified columns:")
    print(f"  Ticker: {ticker_col}")
    print(f"  Expiry: {expiry_col}")
    print(f"  Call Qty: {call_qty_col}")
    print(f"  Put Qty: {put_qty_col}")
    print(f"  Call $: {call_dollar_col}")
    print(f"  Put $: {put_dollar_col}")

    # Filter out rows with missing ticker
    if ticker_col:
        df = df[df[ticker_col].notna()]

    # Clean and convert numeric columns
    if call_qty_col:
        df[call_qty_col] = pd.to_numeric(df[call_qty_col].astype(str).str.replace(',', ''), errors='coerce').fillna(0)
    if put_qty_col:
        df[put_qty_col] = pd.to_numeric(df[put_qty_col].astype(str).str.replace(',', ''), errors='coerce').fillna(0)
    if call_dollar_col:
        df[call_dollar_col] = pd.to_numeric(df[call_dollar_col].astype(str).str.replace('$', '').str.replace(',', ''), errors='coerce').fillna(0)
    if put_dollar_col:
        df[put_dollar_col] = pd.to_numeric(df[put_dollar_col].astype(str).str.replace('$', '').str.replace(',', ''), errors='coerce').fillna(0)

    # Create standardized columns
    df['call_qty'] = df[call_qty_col] if call_qty_col else 0
    df['put_qty'] = df[put_qty_col] if put_qty_col else 0
    df['call_dollar'] = df[call_dollar_col] if call_dollar_col else 0
    df['put_dollar'] = df[put_dollar_col] if put_dollar_col else 0
    df['total_dollar'] = df['call_dollar'] + df['put_dollar']
    df['total_qty'] = df['call_qty'] + df['put_qty']

    # Create aggregation groups
    group_cols = [c for c in [ticker_col, expiry_col] if c]

    if not group_cols:
        print("Error: Cannot aggregate without ticker")
        return pd.DataFrame()

    # Aggregate
    agg_dict = {
        'call_qty': 'sum',
        'put_qty': 'sum',
        'call_dollar': 'sum',
        'put_dollar': 'sum',
        'total_dollar': 'sum',
        'total_qty': 'sum'
    }

    aggregated = df.groupby(group_cols).agg(agg_dict).reset_index()

    return aggregated

def main():
    """Main execution - processes raw sheet data"""
    print("=== Processing Option Flow Data ===\n")

    # Load raw data
    input_path = '.tmp/raw_sheet_data.json'
    with open(input_path, 'r') as f:
        raw_data = json.load(f)

    # Load config
    with open('config.json', 'r') as f:
        config = json.load(f)

    days_back = config.get('date_filter', {}).get('days_back', 15)

    # Process buying and selling data
    buying_df = process_range_data(raw_data['buying'], 'buying')
    selling_df = process_range_data(raw_data['selling'], 'selling')

    print(f"Buying data: {len(buying_df)} rows, {len(buying_df.columns)} columns")
    print(f"Selling data: {len(selling_df)} rows, {len(selling_df.columns)} columns")

    # Combine data
    combined_df = pd.concat([buying_df, selling_df], ignore_index=True)
    print(f"\nCombined: {len(combined_df)} total rows")

    # Aggregate
    aggregated_df = aggregate_option_flow(combined_df, days_back=days_back)

    # Save processed data
    output_path = '.tmp/option_flow_data.csv'
    aggregated_df.to_csv(output_path, index=False)

    print(f"\n✓ Processed data saved to {output_path}")
    print(f"  Total unique ticker/expiry combinations: {len(aggregated_df)}")

    # Print summary
    if len(aggregated_df) > 0:
        print("\n=== Top 10 by Dollar Amount ===")
        if 'Premium' in aggregated_df.columns or 'Amount' in aggregated_df.columns:
            dollar_col = 'Premium' if 'Premium' in aggregated_df.columns else 'Amount'
            top_10 = aggregated_df.nlargest(10, dollar_col)
            print(top_10.to_string(index=False))

    return aggregated_df

if __name__ == "__main__":
    main()
