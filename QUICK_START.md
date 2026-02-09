# Quick Start Guide

## First Time Setup (5 minutes)

1. **Install dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

2. **Get Google Sheets credentials:**
   - Go to [Google Cloud Console](https://console.cloud.google.com/apis/credentials)
   - Create OAuth 2.0 Client ID (Desktop app)
   - Download as `credentials.json` in this directory

3. **Run the analysis:**
   ```bash
   python run_analysis.py
   ```

   On first run, you'll authenticate via browser. Future runs use the saved token.

## Every Time After

Just run:
```bash
python run_analysis.py
```

## View Results

Check `.tmp/` directory for:
- `dollar_flow_chart.png` - Dollar flow by ticker
- `call_put_qty_chart.png` - Call vs Put quantities
- `option_flow_data.csv` - Raw aggregated data

## What It Does

1. Reads your Google Sheet (buying range A3:Q, selling range S3:AI)
2. Filters for orders from past 15 days (any expiry)
3. Aggregates by ticker and expiry date
4. Generates visualizations showing:
   - Total dollar flow per ticker
   - Call vs Put contract quantities

## Customize

Edit [`config.json`](config.json) to change:
- Sheet URL and ranges
- Number of days to look back (default: 15)
- Aggregation settings

## Troubleshooting

**Can't find credentials.json?**
Download from Google Cloud Console → APIs & Services → Credentials

**No data showing?**
- Verify sheet URL in config.json
- Check that you have access to the sheet
- Ensure there's data from the past 15 days

**Wrong columns?**
The script auto-detects common column names. If it can't find your columns, check the output messages for available column names.

---

For detailed setup: see [SETUP.md](SETUP.md)
For architecture: see [CLAUDE.md](CLAUDE.md) and [workflows/analyze_option_flow.md](workflows/analyze_option_flow.md)
