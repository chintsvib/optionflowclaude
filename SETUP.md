# Setup Instructions

Follow these steps to set up and run the Option Flow Analysis project.

## 1. Install Python Dependencies

```bash
pip install -r requirements.txt
```

## 2. Set Up Google Sheets API Access

### A. Create Google Cloud Project

1. Go to [Google Cloud Console](https://console.cloud.google.com/)
2. Create a new project (or select existing one)
3. Enable the Google Sheets API:
   - Navigate to "APIs & Services" > "Library"
   - Search for "Google Sheets API"
   - Click "Enable"

### B. Create OAuth Credentials

1. Go to "APIs & Services" > "Credentials"
2. Click "+ CREATE CREDENTIALS" > "OAuth client ID"
3. If prompted, configure the OAuth consent screen:
   - Choose "External" user type
   - Fill in required fields (app name, user support email, developer email)
   - Add your email as a test user
   - Save and continue through the scopes (no changes needed)
4. Create OAuth client ID:
   - Application type: "Desktop app"
   - Name: "OptionFlowClaude" (or any name)
   - Click "Create"
5. Download the JSON file and save it as `credentials.json` in the project root

### C. First Run Authentication

On first run, the script will:
1. Open a browser window for Google OAuth authentication
2. Ask you to sign in and grant permissions
3. Save the token to `token.json` for future runs

## 3. Configure Your Sheet

The sheet configuration is in [`config.json`](config.json). Update if needed:

```json
{
  "allDay": {
    "sheet_url": "YOUR_SHEET_URL",
    "sheet_name": "YOUR_SHEET_TAB_NAME",
    "header_row": 3,
    "range_buying": "A3:Q",
    "range_selling": "S3:AI"
  }
}
```

## 4. Run the Analysis

### Option A: Run Complete Pipeline

```bash
python run_analysis.py
```

This will execute all three steps:
1. Read data from Google Sheets
2. Process and aggregate the data (filter past 15 days)
3. Generate visualizations

### Option B: Run Individual Steps

```bash
# Step 1: Read data
python tools/read_google_sheet.py

# Step 2: Process data
python tools/process_option_data.py

# Step 3: Create visualizations
python tools/visualize_option_flow.py
```

## 5. View Results

After running the analysis, check the `.tmp/` directory:

- **`dollar_flow_chart.png`** - Bar chart showing dollar flow by ticker
- **`call_put_qty_chart.png`** - Stacked bar chart of call vs put quantities
- **`option_flow_data.csv`** - Processed and aggregated data

## Troubleshooting

### "credentials.json not found"
Download OAuth credentials from Google Cloud Console (see step 2B above).

### "Permission denied" errors
Make sure the scripts are executable:
```bash
chmod +x tools/*.py run_analysis.py
```

### Can't access the Google Sheet
Ensure:
1. The Google account you authenticated with has access to the sheet
2. The sheet URL in `config.json` is correct
3. The sheet name matches exactly (case-sensitive)

### No data in visualizations
Check:
1. The column ranges in `config.json` are correct
2. There is data from the past 15 days in your sheet
3. Column names match expected values (Ticker, Date, Type, Quantity, Premium, etc.)

### Date filtering not working
The script tries multiple date formats. If your dates aren't being parsed:
1. Check the date format in your sheet
2. Update `parse_date()` in `tools/process_option_data.py` to add your format

## Next Steps

Once everything is working:
1. Adjust the `days_back` in `config.json` to change the date filter
2. Modify visualization settings in `tools/visualize_option_flow.py`
3. Add more charts or analysis as needed
4. Set up automated runs (cron job, scheduled task, etc.)
