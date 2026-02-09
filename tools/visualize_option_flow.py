#!/usr/bin/env python3
"""
Tool: Visualize Option Flow
Description: Generates charts and visualizations for option flow data
"""

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

# Set style
sns.set_style("whitegrid")
plt.rcParams['figure.figsize'] = (14, 8)

def create_dollar_flow_chart(df, output_path='.tmp/dollar_flow_chart.png', top_n=20):
    """
    Create bar chart showing dollar flow by ticker

    Args:
        df: Aggregated DataFrame
        output_path: Where to save the chart
        top_n: Show top N tickers by dollar amount
    """
    # Find dollar column
    dollar_cols = ['total_dollar', 'Premium', 'Amount', 'Dollar Amount', 'Value', 'premium', 'amount']
    dollar_col = next((c for c in dollar_cols if c in df.columns), None)

    if not dollar_col:
        print(f"Error: No dollar amount column found. Available: {df.columns.tolist()}")
        return

    # Find ticker column
    ticker_cols = ['Ticker', 'Symbol', 'ticker', 'symbol', 'Stock']
    ticker_col = next((c for c in ticker_cols if c in df.columns), None)

    if not ticker_col:
        print(f"Error: No ticker column found. Available: {df.columns.tolist()}")
        return

    # Aggregate by ticker (sum across all expiries)
    ticker_flow = df.groupby(ticker_col)[dollar_col].sum().sort_values(ascending=False)

    # Take top N
    ticker_flow = ticker_flow.head(top_n)

    # Create chart
    fig, ax = plt.subplots(figsize=(14, 8))

    bars = ax.barh(range(len(ticker_flow)), ticker_flow.values, color='steelblue')

    # Color positive/negative differently if there are negative values
    for i, (ticker, value) in enumerate(ticker_flow.items()):
        if value < 0:
            bars[i].set_color('lightcoral')

    ax.set_yticks(range(len(ticker_flow)))
    ax.set_yticklabels(ticker_flow.index)
    ax.set_xlabel('Dollar Flow ($)', fontsize=12)
    ax.set_ylabel('Ticker', fontsize=12)
    ax.set_title(f'Option Flow by Ticker - Top {len(ticker_flow)} (Past 15 Days)', fontsize=14, fontweight='bold')

    # Add value labels
    for i, value in enumerate(ticker_flow.values):
        label = f'${value:,.0f}'
        ax.text(value, i, label, va='center', ha='left' if value >= 0 else 'right', fontsize=9)

    plt.tight_layout()
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    print(f"✓ Dollar flow chart saved to {output_path}")
    plt.close()

def create_call_put_chart(df, output_path='.tmp/call_put_qty_chart.png', top_n=20):
    """
    Create stacked bar chart showing call vs put quantities by ticker

    Args:
        df: Aggregated DataFrame
        output_path: Where to save the chart
        top_n: Show top N tickers by total quantity
    """
    # Find quantity columns
    if 'call_qty' not in df.columns or 'put_qty' not in df.columns:
        print("Error: call_qty and put_qty columns not found")
        print(f"Available columns: {df.columns.tolist()}")
        return

    # Find ticker column
    ticker_cols = ['Ticker', 'Symbol', 'ticker', 'symbol', 'Stock']
    ticker_col = next((c for c in ticker_cols if c in df.columns), None)

    if not ticker_col:
        print(f"Error: No ticker column found. Available: {df.columns.tolist()}")
        return

    # Aggregate by ticker
    ticker_calls = df.groupby(ticker_col)['call_qty'].sum()
    ticker_puts = df.groupby(ticker_col)['put_qty'].sum()

    # Combine and get total
    ticker_data = pd.DataFrame({
        'calls': ticker_calls,
        'puts': ticker_puts,
        'total': ticker_calls + ticker_puts
    }).sort_values('total', ascending=False)

    # Take top N
    ticker_data = ticker_data.head(top_n)

    # Create chart
    fig, ax = plt.subplots(figsize=(14, 8))

    y_pos = range(len(ticker_data))

    # Create horizontal stacked bars
    ax.barh(y_pos, ticker_data['calls'], label='Calls', color='green', alpha=0.7)
    ax.barh(y_pos, ticker_data['puts'], left=ticker_data['calls'], label='Puts', color='red', alpha=0.7)

    ax.set_yticks(y_pos)
    ax.set_yticklabels(ticker_data.index)
    ax.set_xlabel('Quantity (Contracts)', fontsize=12)
    ax.set_ylabel('Ticker', fontsize=12)
    ax.set_title(f'Call vs Put Quantity by Ticker - Top {len(ticker_data)} (Past 15 Days)', fontsize=14, fontweight='bold')
    ax.legend(loc='lower right')

    # Add value labels
    for i, (idx, row) in enumerate(ticker_data.iterrows()):
        # Call label
        if row['calls'] > 0:
            ax.text(row['calls']/2, i, f"{int(row['calls'])}", va='center', ha='center', fontsize=9, color='white', fontweight='bold')
        # Put label
        if row['puts'] > 0:
            ax.text(row['calls'] + row['puts']/2, i, f"{int(row['puts'])}", va='center', ha='center', fontsize=9, color='white', fontweight='bold')

    plt.tight_layout()
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    print(f"✓ Call/Put quantity chart saved to {output_path}")
    plt.close()

def create_summary_stats(df):
    """Print summary statistics"""
    print("\n=== Summary Statistics ===")

    ticker_cols = ['Ticker', 'Symbol', 'ticker', 'symbol', 'Stock']
    ticker_col = next((c for c in ticker_cols if c in df.columns), None)

    if ticker_col:
        print(f"Unique tickers: {df[ticker_col].nunique()}")

    if 'call_qty' in df.columns and 'put_qty' in df.columns:
        total_calls = df['call_qty'].sum()
        total_puts = df['put_qty'].sum()
        print(f"Total call contracts: {total_calls:,.0f}")
        print(f"Total put contracts: {total_puts:,.0f}")
        if total_calls + total_puts > 0:
            call_ratio = total_calls / (total_calls + total_puts) * 100
            print(f"Call/Put ratio: {call_ratio:.1f}% calls, {100-call_ratio:.1f}% puts")

    dollar_cols = ['total_dollar', 'Premium', 'Amount', 'Dollar Amount', 'Value', 'premium', 'amount']
    dollar_col = next((c for c in dollar_cols if c in df.columns), None)

    if dollar_col:
        total_flow = df[dollar_col].sum()
        print(f"Total dollar flow: ${total_flow:,.2f}")

    if 'call_dollar' in df.columns and 'put_dollar' in df.columns:
        total_call_dollars = df['call_dollar'].sum()
        total_put_dollars = df['put_dollar'].sum()
        print(f"Total call dollars: ${total_call_dollars:,.2f}")
        print(f"Total put dollars: ${total_put_dollars:,.2f}")

def main():
    """Main execution - creates visualizations"""
    print("=== Creating Option Flow Visualizations ===\n")

    # Load processed data
    input_path = '.tmp/option_flow_data.csv'
    df = pd.read_csv(input_path)

    print(f"Loaded {len(df)} rows from {input_path}")

    # Create visualizations
    create_dollar_flow_chart(df)
    create_call_put_chart(df)

    # Print summary
    create_summary_stats(df)

    print("\n✓ All visualizations created successfully!")

if __name__ == "__main__":
    main()
