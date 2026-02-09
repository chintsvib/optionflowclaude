#!/usr/bin/env python3
"""
Tool: Read Google Sheet
Description: Fetches data from Google Sheets using the Sheets API
"""

import os
import json
import pickle
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Scopes required for reading Google Sheets
SCOPES = ['https://www.googleapis.com/auth/spreadsheets.readonly']

def get_credentials():
    """Get or refresh Google API credentials.

    Supports two modes:
      1. Local: reads/writes token.json file (interactive browser auth)
      2. Cloud: reads GOOGLE_TOKEN_JSON env var (no browser needed)
    """
    creds = None

    # Try env var first (cloud mode — Railway, Docker, etc.)
    token_json_env = os.getenv("GOOGLE_TOKEN_JSON")
    if token_json_env:
        creds = Credentials.from_authorized_user_info(json.loads(token_json_env), SCOPES)
    # Fall back to local token file
    elif os.path.exists('token.json'):
        creds = Credentials.from_authorized_user_info(json.load(open('token.json')), SCOPES)

    # If there are no valid credentials, refresh or re-auth
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            if not os.path.exists('credentials.json'):
                raise FileNotFoundError(
                    "credentials.json not found. Please download it from Google Cloud Console:\n"
                    "1. Go to https://console.cloud.google.com/apis/credentials\n"
                    "2. Create OAuth 2.0 Client ID (Desktop app)\n"
                    "3. Download and save as credentials.json"
                )
            flow = InstalledAppFlow.from_client_secrets_file('credentials.json', SCOPES)
            creds = flow.run_local_server(port=0)

        # Save the credentials for the next run (local mode only)
        if not token_json_env:
            with open('token.json', 'w') as token:
                token.write(creds.to_json())

    return creds

def extract_sheet_id(url):
    """Extract spreadsheet ID from Google Sheets URL"""
    # URL format: https://docs.google.com/spreadsheets/d/{SHEET_ID}/edit...
    if '/d/' in url:
        return url.split('/d/')[1].split('/')[0]
    return url

def read_sheet(sheet_url, sheet_name, range_spec):
    """
    Read data from a Google Sheet

    Args:
        sheet_url: Full URL or ID of the spreadsheet
        sheet_name: Name of the sheet tab
        range_spec: Range in A1 notation (e.g., 'A3:Q')

    Returns:
        List of rows, each row is a list of cell values
    """
    try:
        creds = get_credentials()
        service = build('sheets', 'v4', credentials=creds)

        sheet_id = extract_sheet_id(sheet_url)
        range_name = f"{sheet_name}!{range_spec}"

        print(f"Reading from sheet: {sheet_id}")
        print(f"Range: {range_name}")

        result = service.spreadsheets().values().get(
            spreadsheetId=sheet_id,
            range=range_name
        ).execute()

        values = result.get('values', [])
        print(f"Retrieved {len(values)} rows")

        return values

    except HttpError as error:
        print(f"An error occurred: {error}")
        raise
    except Exception as error:
        print(f"An unexpected error occurred: {error}")
        raise

def main():
    """Main execution - reads both buying and selling ranges"""
    # Load configuration
    with open('config.json', 'r') as f:
        config = json.load(f)

    sheet_config = config['allDay']

    print("=== Reading Option Flow Data from Google Sheets ===\n")

    # Read buying range
    print("Reading BUYING range...")
    buying_data = read_sheet(
        sheet_config['sheet_url'],
        sheet_config['sheet_name'],
        sheet_config['range_buying']
    )

    # Read selling range
    print("\nReading SELLING range...")
    selling_data = read_sheet(
        sheet_config['sheet_url'],
        sheet_config['sheet_name'],
        sheet_config['range_selling']
    )

    # Save raw data to .tmp/
    output_data = {
        'buying': buying_data,
        'selling': selling_data,
        'config': sheet_config
    }

    output_path = '.tmp/raw_sheet_data.json'
    with open(output_path, 'w') as f:
        json.dump(output_data, f, indent=2)

    print(f"\n✓ Raw data saved to {output_path}")
    print(f"  - Buying rows: {len(buying_data)}")
    print(f"  - Selling rows: {len(selling_data)}")

    return output_data

if __name__ == "__main__":
    main()
