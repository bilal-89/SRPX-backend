import pandas as pd
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from google.oauth2 import service_account


def get_google_sheet_data(sheet_id, range_name):
    creds = service_account.Credentials.from_service_account_file(
        '/Users/vincentparis/Documents/MAPPING/mapping-429718-c71cbc95e0cb.json',
        scopes=['https://www.googleapis.com/auth/spreadsheets.readonly']
    )
    service = build('sheets', 'v4', credentials=creds)
    sheet = service.spreadsheets()
    result = sheet.values().get(spreadsheetId=sheet_id, range=range_name).execute()
    values = result.get('values', [])

    if not values:
        print('No data found.')
        return None

    df = pd.DataFrame(values[1:], columns=values[0])
    return df

def prepare_data_for_qgis(sheet_id, range_name, output_file):
    df = get_google_sheet_data(sheet_id, range_name)

    if df is None:
        return

    print("Columns found in the sheet:", df.columns.tolist())

    # Remove the Timestamp column if it exists
    timestamp_columns = [col for col in df.columns if 'timestamp' in col.lower()]
    if timestamp_columns:
        df = df.drop(columns=timestamp_columns)
        print(f"Removed timestamp column(s): {timestamp_columns}")

    # Try to identify the required columns
    time_col = next((col for col in df.columns if 'time' in col.lower()), None)
    activity_type_col = next((col for col in df.columns if 'type' in col.lower()), None)
    activity_size_col = next((col for col in df.columns if 'size' in col.lower()), None)

    if not all([time_col, activity_type_col, activity_size_col]):
        print("Error: Could not identify all required columns.")
        print(f"Time column found: {time_col}")
        print(f"Activity Type column found: {activity_type_col}")
        print(f"Activity Size column found: {activity_size_col}")
        return

    # Rename columns to expected names
    df = df.rename(columns={
        time_col: 'Time',
        activity_type_col: 'ActivityType',
        activity_size_col: 'ActivitySize'
    })

    # Convert Time to string in YYYY-MM-DD format
    df['Time'] = pd.to_datetime(df['Time']).dt.strftime('%Y-%m-%d')

    # Function to assign lat and long based on ActivityType
    def assign_coords(activity_type):
        if isinstance(activity_type, str) and activity_type.lower() == 'jyg':
            return 39.9487, -75.2344  # Coordinates for JYG
        else:
            return None, None  # You can add more conditions for other activity types

    # Apply the function to create new Latitude and Longitude columns
    df['Latitude'], df['Longitude'] = zip(*df['ActivityType'].apply(assign_coords))

    # Ensure ActivitySize is numeric
    df['ActivitySize'] = pd.to_numeric(df['ActivitySize'], errors='coerce')

    # Reorder columns
    columns_order = ['Time', 'Longitude', 'Latitude', 'ActivityType', 'ActivitySize']
    df = df[columns_order]

    # Save to CSV
    df.to_csv(output_file, index=False)
    print(f"CSV file created: {output_file}")

# Example usage
sheet_id = '175yVQ09zu9JpBuKOCUq92QoVJfvvHMh3MqDU-FEEr4g'
range_name = 'INPUT!A:Z'  # This will get all columns
output_file = '/Users/vincentparis/Documents/MAPPING/output/qgis_data.csv'
prepare_data_for_qgis(sheet_id, range_name, output_file)