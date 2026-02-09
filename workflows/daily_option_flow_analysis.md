# Workflow: Daily Option Flow Analysis (Enhanced)

## Objective
Analyze option flow with strike-level detail, create interactive visualizations, identify repeated flow patterns, and maintain daily snapshots for trend tracking.

## Required Inputs
- Google Sheets URL and configuration (in `config.json`)
- Google API credentials (`credentials.json` and `token.json`)
- Date range: Past 15 days
- Expiry filter: Next 2 months (current month + next month)

## Tools Used
1. `tools/read_google_sheet.py` - Fetch raw data from Google Sheets
2. `tools/process_detailed_flow.py` - Process with strike-level detail
3. `tools/create_interactive_dashboard.py` - Generate interactive HTML dashboard
4. `tools/save_daily_snapshot.py` - Save daily historical snapshot

## Master Script
Run `python run_daily_analysis.py` to execute the complete pipeline.

## Process

### 1. Read Data from Google Sheets
- Fetch buying range (A3:Q) and selling range (S3:AI)
- Combine into unified dataset
- Cache raw data in `.tmp/raw_sheet_data.json`

### 2. Process with Strike-Level Detail
**Key features:**
- Filter for past 15 days
- **Focus on near-term expiries** (this month + next month only)
- Keep individual order details (ticker, strike, expiry, quantities, dollars)
- Create expiry dates from xMonth/xDate/xYear components
- Identify repeated flows (same ticker/strike/expiry hit multiple times)
- Calculate call/put breakdowns

**Outputs:**
- `.tmp/detailed_flow.csv` - All orders with strike-level detail
- `.tmp/repeated_flows.csv` - Strikes/expiries hit multiple times

### 3. Create Interactive Dashboard
**Features:**
- **Overview**: Top 30 tickers by dollar flow (stacked bar: calls vs puts)
- **Expiry Timeline**: Flow distribution across expiry dates
- **Repeated Flows Table**: Strikes hit multiple times (unusual activity)
- **Per-Ticker Detail** (top 10 tickers):
  - Flow by expiry date
  - Flow by strike price
  - **Heatmaps**: Strike vs Expiry for calls and puts
  - Click/hover for exact values

**Output:**
- `.tmp/option_flow_dashboard.html` - Fully interactive, open in browser

**Interaction:**
- Hover over bars/heatmap cells for details
- Zoom, pan, select regions
- Export to PNG
- All charts responsive and interactive

### 4. Save Daily Snapshot
- Creates `.tmp/snapshots/` directory
- Saves dated copies: `flow_2026-02-01.csv`, `raw_2026-02-01.json`
- Maintains metadata for tracking
- Enables historical comparison and trend analysis

## Expected Outputs

**Daily Interactive Dashboard:**
- `.tmp/option_flow_dashboard.html` (open in browser)
  - Summary statistics
  - Top 30 tickers overview (interactive)
  - Expiry timeline
  - Repeated flows table
  - Detailed analysis for top 10 tickers with heatmaps

**Data Files:**
- `.tmp/detailed_flow.csv` - Strike-level order data
- `.tmp/repeated_flows.csv` - Multi-hit strike/expiry combinations
- `.tmp/snapshots/flow_YYYY-MM-DD.csv` - Daily historical snapshot

## Key Insights Available

### 1. **Repeated Flow Detection**
Identifies strikes/expiries getting hit multiple times - indicates:
- Strong conviction
- Whale activity
- Potential targets

### 2. **Near-Term Focus**
Only shows options expiring in next 2 months:
- More relevant for short-term trading
- Reduces noise from far-dated positions
- Focuses on immediate market views

### 3. **Strike-Level Detail**
See exact strikes being targeted:
- Support/resistance levels
- Hedge levels
- Directional bets

### 4. **Heatmap Visualization**
Visual representation of flow concentration:
- Dark areas = heavy flow
- See clustering patterns
- Identify unusual activity

### 5. **Historical Tracking**
Daily snapshots enable:
- Compare today vs yesterday
- Track flow evolution
- Identify trend changes

## Edge Cases

### Missing Data
- Skip rows with missing ticker or strike
- Handle empty call/put quantities (default to 0)
- Gracefully handle missing expiry data

### Date Parsing
- Supports M/D/YY format (your sheet format)
- Handles 2-digit and 4-digit years
- Filters out unparseable dates

### Expiry Filtering
- Uses python-dateutil for reliable date math
- Calculates "2 months ahead" from today
- Handles month boundaries correctly

### Repeated Flows
- Groups by ticker + expiry + strike + option type
- Only marks as "repeated" if hit 2+ times
- Sorted by hit count (highest first)

### Large Datasets
- All visualizations use Plotly (client-side rendering)
- HTML dashboard is self-contained
- Can handle thousands of data points

## Daily Usage

```bash
# Activate virtual environment
source venv/bin/activate

# Run complete analysis
python run_daily_analysis.py

# Open dashboard
open .tmp/option_flow_dashboard.html
```

## Configuration

Edit `config.json` to adjust:
- `date_filter.days_back`: Number of days to analyze (default: 15)
- `near_term_months`: Months ahead for expiry filter (default: 2)

## Learnings

### 2026-02-01
- Initial implementation of strike-level analysis
- Added near-term expiry filtering (2 months)
- Created interactive Plotly dashboards
- Implemented repeated flow detection
- Added daily snapshot functionality

### Performance Notes
- Processing ~5k orders takes ~2-3 seconds
- Dashboard generation takes ~3-5 seconds for 10 detailed tickers
- HTML file size: ~2-5MB for typical day

### Common Issues
- If no data shows: Check date filter (past 15 days + next 2 months)
- If heatmap empty: Ticker may have no call or put activity
- If repeated flows empty: No strikes hit multiple times in period

## Future Enhancements
- [ ] Add delta/gamma exposure calculations
- [ ] Compare today's flow vs historical averages
- [ ] Add alerts for unusual activity
- [ ] Export to Google Slides automatically
- [ ] Add sector/industry grouping
