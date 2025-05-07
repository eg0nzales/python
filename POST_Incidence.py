import json
import os

with open("Completes_ids.json", "r") as f:
    config = json.load(f)

with open("Directory_Incidence.json", "r") as f:
    directory_incidence = json.load(f)

survey_id_1 = config["survey_id_1"]
survey_id_2 = config["survey_id_2"]
base_folder_path = directory_incidence["base_directory"]

print("Core Survey ID:", survey_id_1)
print("Custom Survey ID:", survey_id_2)
print("Base Directory:", base_folder_path)

start_date = datetime(*time_data['start_date'])  # Unpacks the list into datetime
end_date = datetime(*time_data['end_date'])

import requests
import pandas as pd
from datetime import datetime
from tqdm import tqdm
import io
from time import sleep

# Set up your API key, server domain, and survey IDs
api_key = "uqdpv1ehf12fc3bangxb9kh4m7y0wbp5ffrp87qxt5kssvsxcfncfqm3d4z6dvnm"
server_domain = "https://nrc.decipherinc.com/api/v1/"

# Define the vendor mapping with numerical IDs
vendor_mapping = {
    1: "SSI",
    7: "Cint",
    2: "Toluna",
    6: "Prodege",
    3: "MyPoints"
}

# Define folder paths
vendors_folders = {
    "Cint": "Cint",
    "Prodege": "Prodege and MyPoints",
    "SSI": "SSI",
    "Toluna": "Toluna"
}

# Function to format datetime columns
def format_datetime(value):
    if pd.notnull(value):
        try:
            dt = pd.to_datetime(value, format='%m/%d/%Y %H:%M', errors='coerce')
            return dt.strftime('%-m/%d/%Y %H:%M:00')
        except Exception:
            return value  # Return original value if formatting fails
    return value

# Function to download survey data and return as DataFrame
def download_survey_data(survey_id, survey_type):
    layout = 73 if survey_id == survey_id_1 else 74
    url = f"{server_domain}surveys/{survey_id}/data"
    headers = {"x-apikey": api_key}
    params = {
        "format": "tab",
        "condition": "qualified",
        "layout": layout
    }

    for attempt in range(3):  # Retry logic
        try:
            response = requests.get(url, headers=headers, params=params, timeout=10)
            if response.status_code == 200:
                data = response.text

                # Attempt to read the data
                try:
                    df = pd.read_csv(io.StringIO(data), sep="\t", low_memory=False)
                    print(f"Data downloaded for survey {survey_id}. First few rows:\n{df.head()}")
                except Exception as e:
                    print(f"Failed to parse the response data for survey {survey_id}. Error: {e}")
                    return None

                # Validate schema
                expected_columns = ['Survey end time', 'Survey start time', 'status']
                missing_columns = [col for col in expected_columns if col not in df.columns]
                if missing_columns:
                    print(f"Missing columns {missing_columns} in the data for survey {survey_id}. Skipping this survey.")
                    return None

                return process_survey_data(df, survey_id, survey_type)
            else:
                print(f"Error downloading data for survey {survey_id}: {response.status_code}")
        except requests.exceptions.RequestException as e:
            print(f"Attempt {attempt + 1}: Failed to download data for survey {survey_id}. Error: {e}")
            sleep(2)  # Wait before retrying
    return None

# Function to process survey data
def process_survey_data(df, survey_id, survey_type):
    try:
        # Filter rows with status == 3 at the beginning
        df = df.loc[df['status'] == 3].copy()

        if df.empty:
            print(f"No qualified data found for survey {survey_id}.")
            return None

        if 'Survey end time' not in df.columns:
            if 'Survey start time' in df.columns:
                df['Survey end time'] = df['Survey start time']  # Fallback: Use 'Survey start time' as a placeholder
                print(f"'Survey end time' column inferred from 'Survey start time' for survey {survey_id}.")
            else:
                print(f"Unable to infer 'Survey end time' for survey {survey_id}. Skipping this survey.")
                return None

        # Ensure 'Survey end time' is parsed as datetime
        df['Survey end time'] = pd.to_datetime(df['Survey end time'], errors='coerce')

        # Remove rows where 'Survey end time' is NaT
        df = df.dropna(subset=['Survey end time'])
        if df.empty:
            print(f"All rows have invalid 'Survey end time' values for survey {survey_id}. Skipping this survey.")
            return None

        # Keep only the first 19 columns for Syndicate and 17 columns for Oversample
        if survey_type == "Syndicate":
            df = df.iloc[:, :19]
        elif survey_type == "Oversample":
            df = df.iloc[:, :17]

        # Return the processed DataFrame
        return df
    except Exception as e:
        print(f"An error occurred while processing data for survey {survey_id}: {e}")
        return None

# Function to save the DataFrame to CSV files based on 'Vendor'
def save_files_by_vendor(df, survey_type):
    if df is not None:
        print(f"Saving files by vendor for {survey_type}...")
        vendors = df['Vendor'].unique()
        for vendor_id in tqdm(vendors, desc="Saving files"):
            vendor_name = vendor_mapping.get(vendor_id, 'Unknown')
            if vendor_name != 'Unknown':
                # Determine the folder name (keep SSI as folder name)
                folder_name = vendors_folders.get(vendor_name, 'Unknown')

                # Update SSI to Dynata in file naming
                if vendor_name == "SSI":
                    file_vendor_name = "Dynata"
                else:
                    file_vendor_name = vendor_name

                # Skip MyPoints processing as it will be combined with Prodege
                if vendor_name == "MyPoints":
                    continue

                # Filter data for the current vendor
                vendor_df = df[df['Vendor'] == vendor_id]

                # Determine the base folder path based on survey type
                if survey_type == "Syndicate":
                    base_path = base_folder_path
                elif survey_type == "Oversample":
                    base_path = os.path.join(base_folder_path, "Online Oversample")

                # Determine the folder path for the vendor
                folder_path = os.path.join(base_path, folder_name, '2025')

                # Create folder if it doesn't exist
                if not os.path.exists(folder_path):
                    os.makedirs(folder_path)
                print(f"Creating folder: {folder_path}")

                # Format the date to MM.YYYY
                folder_date = vendor_df['Survey start time'].dt.strftime("%m.%Y").iloc[0]
                date_folder_path = os.path.join(folder_path, folder_date)

                # Create folder if it doesn't exist
                if not os.path.exists(date_folder_path):
                    os.makedirs(date_folder_path)
                print(f"Creating folder: {date_folder_path}")

                # Format the date for the file name
                if survey_type == "Syndicate":
                    month_year = vendor_df['Survey start time'].dt.strftime("%B%y").iloc[0]
                    file_name = f"Incidence.{file_vendor_name}.{month_year}.csv"
                elif survey_type == "Oversample":
                    month_year = vendor_df['Survey start time'].dt.strftime("%B%Y").iloc[0]
                    file_name = f"Incidence.OS.{file_vendor_name}.{month_year}.csv"

                file_path = os.path.join(date_folder_path, file_name)
                print(f"Generating file path: {file_path}")

                # Save DataFrame to CSV file
                vendor_df.to_csv(file_path, index=False)
                print(f"File saved: {file_path}")

        # Combine Prodege and MyPoints data and save
        prodege_df = df[df['Vendor'].isin([6, 3])]  # Prodege and MyPoints
        if not prodege_df.empty:
            # Determine the base folder path based on survey type
            if survey_type == "Syndicate":
                base_path = base_folder_path
            elif survey_type == "Oversample":
                base_path = os.path.join(base_folder_path, "Online Oversample")

            # For Oversample, ensure Prodege folder ends with "Prodege\2025"
            if survey_type == "Oversample":
                folder_path = os.path.join(base_path, "Prodege", '2025')
            else:
                folder_path = os.path.join(base_path, vendors_folders.get("Prodege", "Unknown"), '2025')

            # Create folder if it doesn't exist
            if not os.path.exists(folder_path):
                os.makedirs(folder_path)
            print(f"Creating folder: {folder_path}")

            # Format the date to MM.YYYY
            folder_date = prodege_df['Survey start time'].dt.strftime("%m.%Y").iloc[0]
            date_folder_path = os.path.join(folder_path, folder_date)

            # Create folder if it doesn't exist
            if not os.path.exists(date_folder_path):
                os.makedirs(date_folder_path)
            print(f"Creating folder: {date_folder_path}")

            # Format the date for the file name
            if survey_type == "Syndicate":
                month_year = prodege_df['Survey start time'].dt.strftime("%B%y").iloc[0]
                file_name = f"Incidence.Prodege.{month_year}.csv"
            elif survey_type == "Oversample":
                month_year = prodege_df['Survey start time'].dt.strftime("%B%Y").iloc[0]
                file_name = f"Incidence.OS.Prodege.{month_year}.csv"

            file_path = os.path.join(date_folder_path, file_name)
            print(f"Generating file path: {file_path}")

            # Save combined DataFrame to CSV file
            prodege_df.to_csv(file_path, index=False)
            print(f"File saved: {file_path}")

try:
    core_df = download_survey_data(survey_id_1, "Syndicate")
    custom_df = download_survey_data(survey_id_2, "Oversample")

    if core_df is not None:
        core_df['Survey start time'] = pd.to_datetime(core_df['Survey start time'], format="%m/%d/%Y %H:%M")
        save_files_by_vendor(core_df, "Syndicate")

    if custom_df is not None:
        custom_df['Survey start time'] = pd.to_datetime(custom_df['Survey start time'], format="%m/%d/%Y %H:%M")
        save_files_by_vendor(custom_df, "Oversample")

except Exception as e:
    print(f"An unexpected error occurred: {e}")
