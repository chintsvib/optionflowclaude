#!/usr/bin/env python3
"""
Tool: Create Interactive Dashboard
Description: Creates interactive HTML dashboard with Plotly for option flow analysis
"""

import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
from datetime import datetime
from dotenv import load_dotenv
import argparse

load_dotenv()

def create_ticker_overview(df, top_n=30):
    """Create overview bar chart of top tickers"""
    ticker_summary = df.groupby('ticker').agg({
        'total_dollar': 'sum',
        'call_dollar': 'sum',
        'put_dollar': 'sum',
        'total_qty': 'sum'
    }).sort_values('total_dollar', ascending=False).head(top_n).reset_index()

    fig = go.Figure()

    fig.add_trace(go.Bar(
        y=ticker_summary['ticker'],
        x=ticker_summary['call_dollar'],
        name='Call $',
        orientation='h',
        marker_color='green',
        hovertemplate='<b>%{y}</b><br>Calls: $%{x:,.0f}<extra></extra>'
    ))

    fig.add_trace(go.Bar(
        y=ticker_summary['ticker'],
        x=ticker_summary['put_dollar'],
        name='Put $',
        orientation='h',
        marker_color='red',
        hovertemplate='<b>%{y}</b><br>Puts: $%{x:,.0f}<extra></extra>'
    ))

    fig.update_layout(
        title=f'Top {top_n} Tickers by Dollar Flow (Last 15 Days, Near-Term Expiries)',
        xaxis_title='Dollar Flow ($)',
        yaxis_title='Ticker',
        barmode='stack',
        height=800,
        hovermode='closest',
        template='plotly_white'
    )

    return fig

def create_ticker_detail(df, ticker):
    """Create detailed view for a specific ticker"""
    ticker_data = df[df['ticker'] == ticker].copy()

    # Filter out rows with no dollar values (qty only, no dollars)
    ticker_data = ticker_data[(ticker_data['call_dollar'] > 0) | (ticker_data['put_dollar'] > 0)]

    if len(ticker_data) == 0:
        return None

    # Create single chart - just flow by expiry
    fig = go.Figure()

    # Flow by Expiry - Single bar per expiry, divided by sentiment (bullish/bearish)
    # Group by expiry and sentiment to get bullish vs bearish flow
    expiry_sentiment = ticker_data.groupby(['expiry', 'sentiment']).agg({
        'total_dollar': 'sum',
        'total_qty': 'sum'
    }).reset_index()

    # Pivot to get bullish and bearish as columns
    expiry_flow = expiry_sentiment.pivot_table(
        index='expiry',
        columns='sentiment',
        values=['total_dollar', 'total_qty'],
        fill_value=0,
        aggfunc='sum'
    ).reset_index()

    # Flatten column names
    expiry_flow.columns = ['expiry'] + [f'{col[1]}_{col[0]}' if col[0] != 'expiry' else col[0]
                                         for col in expiry_flow.columns[1:]]

    # Calculate bullish and bearish dollar amounts
    expiry_flow['bullish_dollar'] = expiry_flow.get('Bullish_total_dollar', 0)
    expiry_flow['bearish_dollar'] = expiry_flow.get('Bearish_total_dollar', 0)
    expiry_flow['bullish_qty'] = expiry_flow.get('Bullish_total_qty', 0)
    expiry_flow['bearish_qty'] = expiry_flow.get('Bearish_total_qty', 0)
    expiry_flow['total'] = expiry_flow['bullish_dollar'] + expiry_flow['bearish_dollar']

    # Convert expiry to datetime for proper sorting
    from datetime import datetime
    def parse_expiry_date(exp_str):
        try:
            parts = str(exp_str).split('/')
            if len(parts) == 3:
                month, day, year = parts
                if len(year) == 2:
                    year = '20' + year
                return datetime(int(year), int(month), int(day))
        except:
            pass
        return None

    expiry_flow['expiry_dt'] = expiry_flow['expiry'].apply(parse_expiry_date)
    expiry_flow = expiry_flow.dropna(subset=['expiry_dt'])
    expiry_flow = expiry_flow.sort_values('expiry_dt')

    # Format expiry dates for display (keep chronological order)
    expiry_flow['expiry_display'] = expiry_flow['expiry_dt'].dt.strftime('%m/%d/%y')

    # Convert to millions for better readability
    expiry_flow['bullish_million'] = expiry_flow['bullish_dollar'] / 1e6
    expiry_flow['bearish_million'] = expiry_flow['bearish_dollar'] / 1e6
    expiry_flow['total_million'] = expiry_flow['total'] / 1e6

    # Convert to list for consistent data alignment
    expiry_dates = expiry_flow['expiry_display'].tolist()
    bullish_values = expiry_flow['bullish_million'].tolist()
    bearish_values = expiry_flow['bearish_million'].tolist()
    bullish_dollars = expiry_flow['bullish_dollar'].tolist()
    bearish_dollars = expiry_flow['bearish_dollar'].tolist()
    bullish_qtys = expiry_flow['bullish_qty'].tolist()
    bearish_qtys = expiry_flow['bearish_qty'].tolist()

    # Use actual dollar amounts (in millions) for bar size - VERTICAL STACKED BARS
    # Green = Bullish sentiment, Red = Bearish sentiment
    fig.add_trace(
        go.Bar(
            x=expiry_dates,
            y=bullish_values,
            name='Bullish',
            marker_color='seagreen',
            showlegend=True,
            text=[f'${v:.1f}M' if v > 0 else '' for v in bullish_values],
            textposition='inside',
            textfont=dict(color='white', size=12, weight='bold'),
            hovertemplate='<b>%{x}</b><br>Bullish: $%{customdata[0]:,.0f}<br>Qty: %{customdata[1]:,.0f}<extra></extra>',
            customdata=list(zip(bullish_dollars, bullish_qtys))
        )
    )
    fig.add_trace(
        go.Bar(
            x=expiry_dates,
            y=bearish_values,
            name='Bearish',
            marker_color='indianred',
            showlegend=True,
            text=[f'${v:.1f}M' if v > 0 else '' for v in bearish_values],
            textposition='inside',
            textfont=dict(color='white', size=12, weight='bold'),
            hovertemplate='<b>%{x}</b><br>Bearish: $%{customdata[0]:,.0f}<br>Qty: %{customdata[1]:,.0f}<extra></extra>',
            customdata=list(zip(bearish_dollars, bearish_qtys))
        )
    )

    # Add total labels above each bar
    total_values = expiry_flow['total_million'].tolist()
    fig.add_trace(
        go.Scatter(
            x=expiry_dates,
            y=total_values,
            mode='text',
            text=[f'Total:<br>${v:.1f}M' for v in total_values],
            textposition='top center',
            textfont=dict(size=11, weight='bold'),
            showlegend=False,
            hoverinfo='skip'
        )
    )

    # Update layout
    fig.update_layout(
        height=500,
        title_text=f"Options Volume by Expiration Date ($ Millions) - {ticker}",
        title_font_size=15,
        showlegend=True,
        template='plotly_white',
        barmode='stack',
        xaxis=dict(
            title="Expiration Date",
            type='category'
        ),
        yaxis=dict(
            title="Volume ($M)",
            type='linear'
        )
    )

    return fig

def create_repeated_flows_table(repeated_df):
    """Create table showing repeated flows"""
    if len(repeated_df) == 0:
        return None

    fig = go.Figure(data=[go.Table(
        header=dict(
            values=['Ticker', 'Expiry', 'Strike', 'Type', 'Hit Count'],
            fill_color='paleturquoise',
            align='left',
            font=dict(size=12, color='black')
        ),
        cells=dict(
            values=[
                repeated_df['ticker'],
                repeated_df['expiry'],
                repeated_df['strike'],
                repeated_df['option_type'],
                repeated_df['hit_count']
            ],
            fill_color='lavender',
            align='left',
            font=dict(size=11)
        )
    )])

    fig.update_layout(
        title='Repeated Flows - Same Strike/Expiry Hit Multiple Times',
        height=600
    )

    return fig

def create_large_orders_table(df, min_size_millions=5):
    """Create table showing individual orders above threshold"""
    min_size = min_size_millions * 1e6

    # Find orders where either call or put side is above threshold
    large_orders = df[(df['call_dollar'] >= min_size) | (df['put_dollar'] >= min_size)].copy()

    if len(large_orders) == 0:
        return None

    # Sort by date (most recent first)
    large_orders = large_orders.sort_values('parsed_date', ascending=False)

    # Format the data for display
    large_orders['call_display'] = large_orders['call_dollar'].apply(lambda x: f'${x/1e6:.1f}M' if x > 0 else '-')
    large_orders['put_display'] = large_orders['put_dollar'].apply(lambda x: f'${x/1e6:.1f}M' if x > 0 else '-')
    large_orders['total_display'] = large_orders['total_dollar'].apply(lambda x: f'${x/1e6:.1f}M')
    large_orders['date_display'] = large_orders['parsed_date'].dt.strftime('%m/%d/%y')

    # Add sentiment indicator with color
    def sentiment_with_icon(sentiment):
        if sentiment == 'Bullish':
            return '🟢 Bullish'
        elif sentiment == 'Bearish':
            return '🔴 Bearish'
        else:
            return '⚪ ' + str(sentiment)

    large_orders['sentiment_display'] = large_orders['sentiment'].apply(sentiment_with_icon)

    fig = go.Figure(data=[go.Table(
        header=dict(
            values=['Date', 'Ticker', 'Expiry', 'Strike', 'Calls $', 'Puts $', 'Total $', 'Sentiment', 'Type'],
            fill_color='#ff6b6b',
            align='left',
            font=dict(size=12, color='white', family='Arial Black')
        ),
        cells=dict(
            values=[
                large_orders['date_display'],
                large_orders['ticker'],
                large_orders['expiry'],
                large_orders['strike'],
                large_orders['call_display'],
                large_orders['put_display'],
                large_orders['total_display'],
                large_orders['sentiment_display'],
                large_orders['order_type']
            ],
            fill_color='#ffe0e0',
            align='left',
            font=dict(size=11),
            height=30
        )
    )])

    fig.update_layout(
        title=f'🚨 Large Orders (>${min_size_millions}M on either side) - All Tickers | Expiries: Next 5 Months',
        height=max(400, min(1000, len(large_orders) * 35 + 100))
    )

    return fig

def create_expiry_timeline(df):
    """Create timeline showing flow distribution across expiries"""
    expiry_flow = df.groupby(['expiry_date', 'option_type']).agg({
        'total_dollar': 'sum'
    }).reset_index()

    fig = px.bar(
        expiry_flow,
        x='expiry_date',
        y='total_dollar',
        color='option_type',
        title='Flow Distribution Across Expiry Dates',
        labels={'total_dollar': 'Dollar Flow ($)', 'expiry_date': 'Expiry Date'},
        color_discrete_map={'Call': 'green', 'Put': 'red'},
        height=500
    )

    fig.update_layout(template='plotly_white')

    return fig

def create_sentiment_analysis(df):
    """Create sentiment breakdown charts"""
    # Overall sentiment breakdown
    sentiment_flow = df.groupby('sentiment').agg({
        'total_dollar': 'sum',
        'total_qty': 'sum'
    }).reset_index()

    # Create color mapping
    color_map = {
        'Bullish': 'green',
        'Bearish': 'red',
        'Neutral': '#808080',
        'Unknown': '#D3D3D3'
    }

    colors = [color_map.get(s, '#808080') for s in sentiment_flow['sentiment']]

    fig = go.Figure()

    fig.add_trace(go.Bar(
        x=sentiment_flow['sentiment'],
        y=sentiment_flow['total_dollar'],
        marker_color=colors,
        text=[f'${v/1e9:.2f}B' if v > 1e9 else f'${v/1e6:.0f}M' for v in sentiment_flow['total_dollar']],
        textposition='outside',
        hovertemplate='<b>%{x}</b><br>Flow: $%{y:,.0f}<br>Contracts: %{customdata:,.0f}<extra></extra>',
        customdata=sentiment_flow['total_qty']
    ))

    fig.update_layout(
        title='Market Sentiment - Bullish vs Bearish Flow',
        xaxis_title='Sentiment',
        yaxis_title='Dollar Flow ($)',
        template='plotly_white',
        height=500,
        showlegend=False
    )

    return fig

def create_ticker_sentiment_breakdown(df, top_n=20):
    """Create ticker-level sentiment breakdown"""
    ticker_sentiment = df.groupby(['ticker', 'sentiment']).agg({
        'total_dollar': 'sum'
    }).reset_index()

    # Get top tickers by total flow
    top_tickers = df.groupby('ticker')['total_dollar'].sum().sort_values(ascending=False).head(top_n).index

    ticker_sentiment = ticker_sentiment[ticker_sentiment['ticker'].isin(top_tickers)]

    # Pivot for stacked bar
    pivot = ticker_sentiment.pivot(index='ticker', columns='sentiment', values='total_dollar').fillna(0)

    # Sort by total flow
    pivot['total'] = pivot.sum(axis=1)
    pivot = pivot.sort_values('total', ascending=True)
    pivot = pivot.drop('total', axis=1)

    fig = go.Figure()

    # Color mapping
    color_map = {
        'Bullish': 'green',
        'Bearish': 'red',
        'Neutral': '#808080',
        'Unknown': '#D3D3D3'
    }

    for sentiment in pivot.columns:
        if sentiment in pivot.columns:
            fig.add_trace(go.Bar(
                name=sentiment,
                y=pivot.index,
                x=pivot[sentiment],
                orientation='h',
                marker_color=color_map.get(sentiment, '#808080'),
                hovertemplate='<b>%{y}</b><br>' + sentiment + ': $%{x:,.0f}<extra></extra>'
            ))

    fig.update_layout(
        title=f'Top {top_n} Tickers - Bullish vs Bearish Flow',
        xaxis_title='Dollar Flow ($)',
        yaxis_title='Ticker',
        barmode='stack',
        height=800,
        template='plotly_white',
        legend=dict(orientation='h', yanchor='bottom', y=1.02, xanchor='right', x=1)
    )

    return fig

def create_ema_status_html(ticker, ema_df):
    """Create HTML display for EMA status"""
    ticker_ema = ema_df[ema_df['ticker'] == ticker]

    if len(ticker_ema) == 0:
        return ""

    row = ticker_ema.iloc[0]
    price = row['current_price']
    bullish_count = row['bullish_count']
    total_count = row['total_timeframes']

    # Determine overall trend color
    if bullish_count == total_count:
        trend_color = '#28a745'  # Green
        trend_text = '🚀 STRONG BULLISH'
    elif bullish_count >= total_count * 0.67:
        trend_color = '#5cb85c'  # Light green
        trend_text = '📈 BULLISH'
    elif bullish_count >= total_count * 0.5:
        trend_color = '#f0ad4e'  # Orange
        trend_text = '⚖️ NEUTRAL'
    elif bullish_count > 0:
        trend_color = '#d9534f'  # Light red
        trend_text = '📉 BEARISH'
    else:
        trend_color = '#c9302c'  # Dark red
        trend_text = '🔻 STRONG BEARISH'

    html = f"""
    <div style="background: linear-gradient(135deg, {trend_color}20 0%, {trend_color}10 100%);
                border-left: 4px solid {trend_color}; padding: 15px; margin-bottom: 15px; border-radius: 6px;">
        <div style="display: flex; justify-content: space-between; align-items: center;">
            <div>
                <h3 style="margin: 0; color: #333;">{ticker} - Current Price: ${price:.2f}</h3>
                <p style="margin: 5px 0 0 0; color: #666;">EMA Status: {bullish_count}/{total_count} timeframes above 39 EMA</p>
            </div>
            <div style="text-align: right;">
                <span style="background: {trend_color}; color: white; padding: 8px 16px;
                             border-radius: 20px; font-weight: bold; font-size: 14px;">
                    {trend_text}
                </span>
            </div>
        </div>
        <div style="margin-top: 12px; display: flex; gap: 15px; flex-wrap: wrap; font-size: 13px;">
    """

    # Add each timeframe
    timeframes = ['5m', '10m', '1h', '4h', '1d', '1wk']
    for tf in timeframes:
        above = row[f'{tf}_above']
        ema = row[f'{tf}_ema']
        dist = row[f'{tf}_distance_pct']

        if pd.notna(above):
            icon = "✅" if above else "❌"
            color = "#28a745" if above else "#dc3545"
            html += f"""
            <div style="background: white; padding: 6px 12px; border-radius: 4px; border: 1px solid #ddd;">
                <span style="color: {color}; font-weight: bold;">{icon} {tf.upper()}</span>
                <span style="color: #666;"> ${ema:.2f} ({dist:+.1f}%)</span>
            </div>
            """

    html += """
        </div>
    </div>
    """

    return html

def create_full_dashboard(df, repeated_df, output_path=None, days_back=15, df_extended=None):
    """Create comprehensive multi-page dashboard"""
    # Generate timestamped filename if not provided
    if output_path is None:
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        output_path = f'.tmp/option_flow_dashboard_past{days_back}_{timestamp}.html'

    # Use extended dataframe for large orders (5 months), or fall back to regular df
    if df_extended is not None:
        full_df = df_extended.copy()
    else:
        full_df = df.copy()

    # Show all tickers from the sheet (no filtering)

    # Load EMA status data
    try:
        ema_df = pd.read_csv('.tmp/ema_status.csv')
        print(f"Loaded EMA status for {len(ema_df)} tickers")
    except FileNotFoundError:
        ema_df = pd.DataFrame()
        print("No EMA status file found - skipping EMA display")

    # Get top tickers by flow
    top_tickers = df.groupby('ticker')['total_dollar'].sum().sort_values(ascending=False).index.tolist()

    # Create HTML with multiple sections
    html_parts = []

    # Header
    html_parts.append(f"""
    <html>
    <head>
        <title>Option Flow Dashboard - {datetime.now().strftime('%Y-%m-%d')}</title>
        <style>
            body {{ font-family: Arial, sans-serif; margin: 20px; background-color: #f5f5f5; }}
            h1 {{ color: #333; }}
            h2 {{ color: #666; margin-top: 30px; }}
            .dashboard-section {{ background: white; padding: 20px; margin: 20px 0; border-radius: 8px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }}
            .stats {{ display: flex; gap: 20px; flex-wrap: wrap; }}
            .stat-box {{ background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); color: white; padding: 20px; border-radius: 8px; flex: 1; min-width: 200px; }}
            .stat-number {{ font-size: 32px; font-weight: bold; }}
            .stat-label {{ font-size: 14px; opacity: 0.9; }}
        </style>
        <script src="https://cdn.plot.ly/plotly-latest.min.js"></script>
    </head>
    <body>
        <h1>📊 Option Flow Dashboard</h1>
        <p>Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
        <p><strong>Period:</strong> Last {days_back} business days | <strong>Expiries:</strong> Next 2 months</p>
    """)

    # Summary stats - removed per user request
    # Overview sections - removed per user request

    # Repeated flows table
    if len(repeated_df) > 0:
        html_parts.append('<div class="dashboard-section">')
        repeated_fig = create_repeated_flows_table(repeated_df.head(50))
        html_parts.append(repeated_fig.to_html(full_html=False, include_plotlyjs=False))
        html_parts.append('</div>')

    # Large orders table (>$5M on either side) - ALL TICKERS (5-month expiries)
    large_orders_fig = create_large_orders_table(full_df, min_size_millions=5)
    if large_orders_fig:
        html_parts.append('<div class="dashboard-section">')
        html_parts.append(large_orders_fig.to_html(full_html=False, include_plotlyjs=False))
        html_parts.append('</div>')
        print(f"Found {len(full_df[(full_df['call_dollar'] >= 5e6) | (full_df['put_dollar'] >= 5e6)])} large orders (>$5M, 5-month expiries)")

    # Detailed views for top tickers
    html_parts.append('<div class="dashboard-section"><h2>Detailed Ticker Analysis</h2></div>')

    for ticker in top_tickers:
        html_parts.append(f'<div class="dashboard-section">')

        # Add EMA status if available
        if len(ema_df) > 0:
            ema_html = create_ema_status_html(ticker, ema_df)
            html_parts.append(ema_html)

        # Add option flow chart
        detail_fig = create_ticker_detail(df, ticker)
        if detail_fig:
            html_parts.append(detail_fig.to_html(full_html=False, include_plotlyjs=False))
        html_parts.append('</div>')

    html_parts.append('</body></html>')

    # Save
    with open(output_path, 'w') as f:
        f.write('\n'.join(html_parts))

    print(f"✓ Interactive dashboard saved to {output_path}")

    return output_path

def main():
    """Main execution"""
    # Parse command-line arguments
    parser = argparse.ArgumentParser(description='Create interactive option flow dashboard')
    parser.add_argument('--days', type=int, default=15,
                       help='Number of business days back analyzed (default: 15)')
    args = parser.parse_args()

    days_back = args.days

    print("=== Creating Interactive Option Flow Dashboard ===\n")
    print(f"Dashboard for past {days_back} business days analysis")

    # Load detailed data (2 months for main dashboard)
    df = pd.read_csv('.tmp/detailed_flow.csv')
    print(f"Loaded {len(df)} orders (2-month expiries)")

    # Load extended data (5 months) for large orders analysis
    try:
        df_extended = pd.read_csv('.tmp/detailed_flow_extended.csv')
        print(f"Loaded {len(df_extended)} orders (5-month expiries) for large orders analysis")
    except FileNotFoundError:
        print("Extended flow data not found, using regular data for large orders")
        df_extended = df.copy()

    # Load repeated flows
    try:
        repeated_df = pd.read_csv('.tmp/repeated_flows.csv')
        print(f"Loaded {len(repeated_df)} repeated flow patterns")
    except FileNotFoundError:
        repeated_df = pd.DataFrame()
        print("No repeated flows file found")

    # Convert date columns to datetime if they're strings
    if 'expiry_date' in df.columns:
        df['expiry_date'] = pd.to_datetime(df['expiry_date'])
    if 'parsed_date' in df.columns:
        df['parsed_date'] = pd.to_datetime(df['parsed_date'])

    # Convert date columns for extended dataframe
    if 'expiry_date' in df_extended.columns:
        df_extended['expiry_date'] = pd.to_datetime(df_extended['expiry_date'])
    if 'parsed_date' in df_extended.columns:
        df_extended['parsed_date'] = pd.to_datetime(df_extended['parsed_date'])

    # Create timestamped dashboard
    dashboard_path = create_full_dashboard(df, repeated_df, days_back=days_back, df_extended=df_extended)

    print(f"\n✓ Dashboard ready!")
    print(f"  Open file: {dashboard_path}")

    return dashboard_path

if __name__ == "__main__":
    main()
