# Workflow: Analyze Option Flow

## Objective
Read option order data from Google Sheets, filter for recent trades (past 15 days), aggregate by ticker and expiry, and generate visualizations showing dollar flow and call/put quantities.

## Required Inputs
- Google Sheets URL and configuration (stored in `config.json`)
- Date range: Past 15 days from current date
- Google API credentials (via `.env` and OAuth token)

## Tools Used
1. `tools/read_google_sheet.py` - Fetch data from Google Sheets
2. `tools/process_option_data.py` - Filter, aggregate, and analyze option data
3. `tools/visualize_option_flow.py` - Generate charts and visualizations

## Process

### 1. Read Data from Google Sheets
- Use Google Sheets API to fetch both buying and selling ranges
- Combine data from ranges: A3:Q (buying) and S3:AI (selling)
- Header row is at row 3

### 2. Filter and Process Data
- Filter orders from past 15 days (any expiry date)
- Identify ticker, expiry, option type (call/put), quantity, and dollar amount
- Aggregate orders by ticker and expiry:
  - Sum quantities for calls vs puts
  - Sum dollar amounts for each ticker

### 3. Generate Visualizations
- Bar chart: Dollar flow by ticker
- Stacked bar chart: Call vs Put quantities by ticker
- Save to `.tmp/` directory
- Option to upload to Google Drive/Slides

## Expected Outputs
- `.tmp/option_flow_data.csv` - Processed and aggregated data
- `.tmp/dollar_flow_chart.png` - Dollar amount visualization
- `.tmp/call_put_qty_chart.png` - Call/Put quantity visualization
- Console summary of top tickers by flow

## Edge Cases

### Missing Data
- Handle empty cells gracefully
- Skip rows with missing ticker or date information

### Date Parsing
- Support multiple date formats (MM/DD/YYYY, YYYY-MM-DD, etc.)
- Handle timezone differences

### API Rate Limits
- Google Sheets API: 300 requests per 60 seconds per project
- For large datasets, consider caching raw data in `.tmp/`

### Authentication Issues
- If credentials.json missing: Prompt user to download from Google Cloud Console
- If token expired: Re-authenticate via OAuth flow

## Configuration
Sheet parameters stored in `config.json`:
```json
{
  "allDay": {
    "sheet_url": "...",
    "sheet_name": "Prior Orders Data (All)",
    "header_row": 3,
    "range_buying": "A3:Q",
    "range_selling": "S3:AI"
  }
}
```

## Learnings
- Document any column mapping discoveries here
- Note any data quality issues
- Track performance optimizations
