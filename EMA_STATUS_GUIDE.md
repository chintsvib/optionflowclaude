# EMA Status Tracking - Usage Guide

## What's New

Your option flow analysis now includes **39 EMA status tracking** across 6 timeframes:
- ✅ **5 minute**
- ✅ **10 minute** (uses 15m as proxy)
- ✅ **1 hour**
- ✅ **4 hour**
- ✅ **Daily**
- ✅ **Weekly**

## How It Works

For each ticker in your focus list, the system:
1. Fetches current price
2. Calculates 39 EMA for each timeframe
3. Compares price vs EMA
4. Displays results in your dashboard

## Running the Analysis

### Integrated with Daily Pipeline

```bash
# Default (15 days + EMA status)
python run_daily_analysis.py

# Custom lookback (5 days + EMA status)
python run_daily_analysis.py --past 5
```

The EMA fetch is now **Step 3/6** in your pipeline.

### Standalone EMA Tool

You can also run EMA analysis independently:

```bash
source venv/bin/activate
python tools/fetch_ema_status.py
```

This will:
- Fetch EMA status for all 22 focus tickers
- Print detailed results to console
- Save to `.tmp/ema_status.csv`

## Dashboard Display

When you open the dashboard, you'll see for each ticker:

### Visual Status Card
- **Current Price**: Live price from yfinance
- **Overall Trend**: Color-coded status (🚀 STRONG BULLISH to 🔻 STRONG BEARISH)
- **Score**: X/6 timeframes above 39 EMA
- **Individual Timeframes**: Each timeframe with ✅/❌, EMA value, and % distance

### Example Display

```
GOOG - Current Price: $344.90
EMA Status: 6/6 timeframes above 39 EMA
                    🚀 STRONG BULLISH

✅ 5M  $344.02 (+0.3%)
✅ 10M $341.96 (+0.9%)
✅ 1H  $337.68 (+2.1%)
✅ 4H  $330.73 (+4.3%)
✅ 1D  $321.71 (+7.2%)
✅ 1WK $264.30 (+30.5%)
```

## Interpreting Results

### Bullish Signals
- **6/6 or 5/6**: Strong uptrend across all timeframes
- **4/6**: Moderate bullish, some consolidation
- Price far above weekly EMA = long-term strength

### Bearish Signals
- **0/6 or 1/6**: Price below most EMAs, downtrend
- **2/6 or 3/6**: Mixed/transitioning
- Price below daily and lower timeframes = short-term weakness

### Trading Context
- **Option flow + EMA alignment**: Bullish flow + 6/6 EMA = high conviction
- **Divergence**: Bearish flow but 5/6 EMA = potential reversal watch
- **Consolidation**: 3/6 EMA = wait for trend confirmation

## Output Files

### EMA Status CSV
**File**: `.tmp/ema_status.csv`

Contains:
- ticker
- current_price
- For each timeframe: ema, above (boolean), distance_pct
- bullish_count (how many timeframes above EMA)
- total_timeframes

### Dashboard Integration
**File**: `.tmp/option_flow_dashboard.html`

Now includes:
1. EMA status card (top of each ticker section)
2. Option flow by expiry chart (below EMA status)

## Focus Tickers

Currently tracking EMA for:
```
NVDA, GOOG, GOOGL, AAPL, MSFT, AMZN, META, AVGO, TSM, TSLA,
SPY, QQQ, SPX, NDX, PLTR, AMD, UBER, QCOM, GLD, SLV, MU, NOW
```

## Example Use Cases

### 1. Pre-Market Scan
```bash
python tools/fetch_ema_status.py
```
Quickly see which tickers are above all EMAs before market open.

### 2. Daily Analysis
```bash
python run_daily_analysis.py
```
Get option flow + EMA status in one comprehensive dashboard.

### 3. Quick Check on Specific Ticker
Look at the dashboard, find the ticker, check:
- Score (X/6)
- Overall trend color
- Which timeframes are aligned

### 4. Identify Strong Setups
Look for:
- High bullish option flow (green bars)
- 5/6 or 6/6 EMA status
- Recent repeated flows at key strikes

## Troubleshooting

### EMA data missing
- Check if `.tmp/ema_status.csv` exists
- Run `python tools/fetch_ema_status.py` manually
- Verify yfinance is installed: `pip list | grep yfinance`

### Ticker shows 0/0 or no data
- Ticker may not have intraday data available (e.g., SPX, NDX)
- Some tickers don't support 5m/10m intervals
- This is normal for certain indices

### Dashboard doesn't show EMA status
- Make sure pipeline ran successfully
- Check console for "Loaded EMA status for X tickers"
- Verify `.tmp/ema_status.csv` was created

## Technical Notes

### Data Source
- **yfinance**: Free Yahoo Finance API
- **Timeframes**: 5m (5d history), 1h (1mo), 1d (6mo), 1wk (2y)
- **Current Price**: Most recent 5m candle close

### EMA Calculation
- Standard exponential moving average
- 39 period (configurable in code)
- Pandas `.ewm(span=39, adjust=False)`

### Performance
- Fetches ~22 tickers in ~30-60 seconds
- Cached data not used (always fresh)
- Runs once per pipeline execution

## Next Steps

1. Run the analysis: `python run_daily_analysis.py`
2. Open the dashboard: `.tmp/option_flow_dashboard.html`
3. Look for tickers with:
   - Strong EMA alignment (5/6 or 6/6)
   - Matching bullish option flow
   - Repeated flows at key strikes

This combination provides the highest conviction trade setups.

---

**Last Updated**: 2026-02-02
