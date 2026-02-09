# Option Flow Analysis - Usage Guide

## What You Have Now

An **interactive daily option flow analysis system** that:
- ✅ Pulls data from Google Sheets automatically
- ✅ Filters for past 15 days + near-term expiries (next 2 months)
- ✅ Analyzes at strike-level detail
- ✅ Identifies repeated flows (same strike hit multiple times)
- ✅ Creates **interactive HTML dashboards**
- ✅ Saves daily snapshots for historical tracking

## Daily Workflow

### Run the Analysis

```bash
# Activate virtual environment
source venv/bin/activate

# Run complete pipeline
python run_daily_analysis.py
```

This executes 4 steps:
1. Fetch latest data from Google Sheets
2. Process with strike-level detail (filter: last 15 days, next 2 months expiries)
3. Create interactive dashboard
4. Save daily snapshot

### View Results

**Open the interactive dashboard:**
```bash
open .tmp/option_flow_dashboard.html
```

or just double-click: [.tmp/option_flow_dashboard.html](.tmp/option_flow_dashboard.html)

## What's in the Dashboard

### 📊 Summary Statistics
- Total dollar flow
- Number of unique tickers
- Total orders
- Call/Put ratio

### 📈 Top Tickers Overview
- **Interactive bar chart** showing top 30 tickers
- Stacked bars: Green (calls) vs Red (puts)
- Hover for exact dollar amounts
- Sortable and zoomable

### 📅 Expiry Timeline
- Shows flow distribution across expiry dates
- Color-coded by call/put
- See which expiry dates are getting the most flow

### 🔁 Repeated Flows Table
**KEY FEATURE** - Shows strikes hit multiple times:
- Ticker, Expiry, Strike, Option Type
- **Hit Count** = how many times this exact position was established
- Sorted by hit count (most repeated first)
- **High hit counts = strong conviction or whale activity**

### 🎯 Detailed Ticker Analysis (Top 10)

For each of the top 10 tickers, you get 4 visualizations:

1. **Flow by Expiry** - Which expiry dates are getting flow
2. **Flow by Strike** - Which strikes are being targeted
3. **Strike vs Expiry Heatmap (Calls)** - Visual map showing call concentration
4. **Strike vs Expiry Heatmap (Puts)** - Visual map showing put concentration

**Heatmaps are interactive:**
- Darker color = more flow
- Hover to see exact dollar amounts
- Zoom and pan
- Instantly see clustering patterns

## Example Insights from Today's Run

Based on your latest data:

### Top Flow (Near-Term Only)
1. **SLV**: $354M (mostly calls at $90 and $110 strikes for Mar expiry)
2. **GLD**: $132M (calls concentrated at $380 strike for Feb)
3. **SPY**: $121M (mostly puts around $657 strike)

### Most Repeated Flows
- **GLD $380 Call (Feb 20)**: Hit **25 times** 🚨
- **IWM $250 Put (Feb 20)**: Hit 19 times
- **SLV $90 Call (Mar 20)**: Hit 16 times

**These repeated flows indicate strong conviction or coordinated positioning.**

### Filtering Applied
- Orders from: Jan 17 - Feb 1 (past 15 days)
- Expiries: Feb 1 - Apr 1 (next 2 months only)
- **2,605 orders** analyzed across **208 tickers**

## Output Files

### Interactive Dashboard
- **File**: `.tmp/option_flow_dashboard.html` (187KB)
- **Format**: Self-contained HTML with embedded Plotly charts
- **Share**: Can email or share this single file
- **Works offline**: No internet needed to view

### Data Files
- `.tmp/detailed_flow.csv` (234KB) - All 2,605 orders with strike detail
- `.tmp/repeated_flows.csv` (11KB) - 462 repeated flow patterns
- `.tmp/snapshots/flow_2026-02-01.csv` - Today's snapshot

### Historical Tracking
Daily snapshots saved in `.tmp/snapshots/`:
- `flow_YYYY-MM-DD.csv` - Each day's processed data
- `raw_YYYY-MM-DD.json` - Each day's raw Sheet data

**Run daily to build history and compare trends.**

## Customization

### Change Time Windows

Edit `config.json`:

```json
{
  "date_filter": {
    "days_back": 15,  // Look back N days
  }
}
```

Edit `tools/process_detailed_flow.py` line 101:
```python
df = filter_near_term_expiries(df, expiry_col, months_ahead=2)  // Change 2 to N
```

### Change Top N Tickers

Edit `tools/create_interactive_dashboard.py` line 212:
```python
top_tickers = df.groupby('ticker')['total_dollar'].sum().sort_values(ascending=False).head(10)
```
Change `head(10)` to `head(N)` for more ticker details.

## Best Practices

### Run Daily
```bash
# Add to crontab or schedule task
0 18 * * * cd /path/to/OptionFlowClaude && source venv/bin/activate && python run_daily_analysis.py
```

### Track Changes
- Compare today's repeated flows vs yesterday
- Look for new tickers entering top 10
- Watch for shifts in call/put ratio

### Focus Areas
1. **Repeated flows with high hit counts** (10+ hits)
2. **Unusual strike clustering** in heatmaps
3. **Sudden changes in expiry distribution**
4. **Large put/call imbalances** on specific strikes

### Save Important Dashboards
```bash
cp .tmp/option_flow_dashboard.html reports/flow_$(date +%Y-%m-%d).html
```

## Troubleshooting

### Dashboard won't open
- Make sure file exists: `ls -lh .tmp/option_flow_dashboard.html`
- Try: `python -m http.server 8000` then open `localhost:8000/.tmp/option_flow_dashboard.html`

### No data showing
- Check date filters (15 days back + 2 months forward)
- Verify Google Sheets has recent data
- Check console output for errors

### Dashboard is slow
- Normal for 200+ tickers
- Consider reducing top_n in overview
- Reduce number of detailed ticker views

## Next Steps

### Today
1. ✅ Open the dashboard: `.tmp/option_flow_dashboard.html`
2. ✅ Review repeated flows table for unusual activity
3. ✅ Check heatmaps for top tickers to see strike clustering

### Going Forward
1. Run daily to build historical snapshots
2. Compare flow changes day-over-day
3. Look for patterns in repeated flows
4. Track which tickers consistently show high flow

### Advanced Usage
- Compare snapshots: `diff .tmp/snapshots/flow_2026-02-01.csv .tmp/snapshots/flow_2026-01-31.csv`
- Export specific tickers for deeper analysis
- Build alerts based on hit counts or dollar thresholds

---

**🎯 Your main file to open daily**: [`.tmp/option_flow_dashboard.html`](.tmp/option_flow_dashboard.html)

See [workflows/daily_option_flow_analysis.md](workflows/daily_option_flow_analysis.md) for technical details.
