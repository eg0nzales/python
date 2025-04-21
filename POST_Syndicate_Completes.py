import json
from datetime import datetime, date 
import os

with open("Completes_ids.json", "r") as f:
    config = json.load(f)

survey_id_1 = config["survey_id_1"]
survey_id_2 = config["survey_id_2"]

print("Core Survey ID:", survey_id_1)
print("Custom Survey ID:", survey_id_2)

with open('Completes_dates.json', 'r') as f:
    time_data = json.load(f)

start_date = datetime(*time_data['start_date'])  # Unpacks the list into datetime
end_date = datetime(*time_data['end_date'])

# Base directory for saving files
base_directory = r"T:\MarketInsights\HCMG2008\Kinesis\Data (Investigate further)"

# Function to construct the directory path based on the date
def construct_directory_path(base_dir, date):
    year = date.strftime("%Y")
    month_year = date.strftime("%m.%Y")
    return os.path.join(base_dir, year, month_year)

# Function to construct the file name based on the format
def construct_file_name(survey_type, survey_date):
    today_date = date.today().strftime("%m%d%y")  # Format today's date as MMDDYY
    year_month = survey_date.strftime("%Y_%m")   # Format survey date as YYYY_MM
    return f"Decipher.{year_month}.{survey_type}.{today_date}"

import requests
import pandas as pd

from tqdm import tqdm
import io
import unicodedata

# Set up your API key, server domain, and survey IDs
api_key = "uqdpv1ehf12fc3bangxb9kh4m7y0wbp5ffrp87qxt5kssvsxcfncfqm3d4z6dvnm"
server_domain = "https://nrc.decipherinc.com/api/v1/"

# Function to format datetime columns
def format_datetime(value):
    if pd.notnull(value):
        try:
            dt = pd.to_datetime(value, format='%m/%d/%Y %H:%M', errors='coerce')
            return dt.strftime('%-m/%d/%Y %H:%M:00')
        except Exception:
            return value  # Return original value if formatting fails
    return value

# Function to clean and normalize text data
def clean_and_normalize_text(value):
    if isinstance(value, str):
        # Replace problematic characters
        value = value.replace("â€™", "'")
        # Normalize to NFC form to standardize characters
        value = unicodedata.normalize('NFC', value)
    return value

# Function to download survey data and return as DataFrame
def download_survey_data(survey_id, survey_type):
    layout = 61 if survey_id == survey_id_1 else 66
    url = f"{server_domain}surveys/{survey_id}/data"
    headers = {"x-apikey": api_key}
            
    params = {
        "format": "tab",        # Format can be tab, csv, json, etc.
        "condition": "qualified",  # Filter responses to only qualified ones
        "layout": layout
    }

    response = requests.get(url, headers=headers, params=params)
    if response.status_code == 200:
        # Decode the response text using utf-8 to ensure consistency
        data = response.content.decode('utf-8')  # Decode as utf-8
        try:
            df = pd.read_csv(io.StringIO(data), sep="\t", low_memory=False)
            if 'endtime' not in df.columns:
                raise KeyError("'endtime' column is missing in the data.")

            df['date'] = pd.to_datetime(df['endtime'], errors='coerce')
            filtered_df = df.loc[(df['status'] == 3) & (df['date'] >= start_date) & (df['date'] <= end_date)].copy()
 
            if filtered_df.empty:
                print(f"No qualified data found for survey {survey_id}.")
                return None, None, None
         
            # Process numeric columns
            for col in filtered_df.columns[4:]:
                if pd.api.types.is_numeric_dtype(filtered_df[col]) and col not in ['QZIPCODE', 'QFIPS']:
                    filtered_df[col] = filtered_df[col].apply(lambda x: '{:.0f}'.format(x) if pd.notnull(x) else x)

            # Format specific columns
            for col in ['QZIPCODE', 'QFIPS']:
                if col in filtered_df.columns:
                    filtered_df[col] = filtered_df[col].apply(
                        lambda x: f"{int(float(x)):05d}" if pd.notnull(x) and str(x).replace('.', '', 1).isdigit() else x
                    )

            for col in ['starttime', 'endtime']:
                if col in filtered_df.columns:
                    filtered_df[col] = filtered_df[col].apply(format_datetime)
                    
            # Truncate 'URI' column for custom survey
            if survey_type == "Custom" and 'URI' in filtered_df.columns:
                filtered_df['URI'] = filtered_df['URI'].str[:60]
                
            # Clean and normalize text data to avoid problematic characters
            for col in filtered_df.columns:
                filtered_df[col] = filtered_df[col].map(clean_and_normalize_text)
            
            # Truncate the "Q0009017T" column to a length limit of 90 characters
            if 'Q0009017T' in filtered_df.columns:
                filtered_df['Q0009017T'] = filtered_df['Q0009017T'].str[:90]
            
            # Convert DataFrame to string type to ensure 'nan' replacement
            filtered_df = filtered_df.astype(str)
            
            # Replace 'nan' text results with a blank
            filtered_df = filtered_df.replace(r'\bnan\b', '', regex=True)
                
            # Ensure 'date' column is datetime before using .dt accessor
            filtered_df['date'] = pd.to_datetime(filtered_df['date'], errors='coerce')
            date = filtered_df['date'].dt.strftime("%B %Y").iloc[0]
            
            # Construct the directory path
            save_directory = construct_directory_path(base_directory, filtered_df['date'].iloc[0])
            os.makedirs(save_directory, exist_ok=True)  # Create the directory if it doesn't exist
            
            # Construct the file name
            file_name = construct_file_name(survey_type, filtered_df['date'].iloc[0])
            csv_file_name = os.path.join(save_directory, f"{file_name}.dat")
            
            # Save the file with utf-8 encoding to ensure compatibility
            filtered_df.to_csv(csv_file_name, sep="\t", index=False, na_rep='', encoding='utf-8')

            print(f"Qualified data downloaded successfully and saved to '{csv_file_name}'")
            return filtered_df, csv_file_name, save_directory
        except Exception as e:
            print(f"An error occurred while processing data for survey {survey_id}: {e}")
            return None, None, None
    else:
        print(f"Error downloading data for survey {survey_id}: {response.status_code}")
        return None, None, None

try:
    core_df, core_csv_file, core_save_directory = download_survey_data(survey_id_1, "Core")

    if core_df is not None and core_csv_file is not None:
        core_record_ids = set(core_df.iloc[:, 0].astype(str))
        
        custom_df, custom_csv_file, custom_save_directory = download_survey_data(survey_id_2, "Custom")

        if custom_df is not None and custom_csv_file is not None:
            # Extract the date value before dropping the 'date' column
            core_date = core_df['date'].iloc[0]  # Extract as datetime object
            custom_date = custom_df['date'].iloc[0]  # Extract as datetime object

            # Perform all operations requiring the 'date' column first
            filtered_custom_df = custom_df[custom_df.iloc[:, 0].astype(str).isin(core_record_ids)]
            removed_custom_df = custom_df[~custom_df.iloc[:, 0].astype(str).isin(core_record_ids)]

            # Debug: Check if 'date' column exists
            print("Before dropping 'date':")
            print("custom_df columns:", custom_df.columns)
            print("core_df columns:", core_df.columns)
            print("filtered_custom_df columns:", filtered_custom_df.columns)
            print("removed_custom_df columns:", removed_custom_df.columns)

            # Explicitly drop the 'date' column from all DataFrames before saving
            if 'date' in custom_df.columns:
                custom_df = custom_df.drop(columns=['date'], errors='ignore')
            if 'date' in core_df.columns:
                core_df = core_df.drop(columns=['date'], errors='ignore')
            if 'date' in filtered_custom_df.columns:
                filtered_custom_df = filtered_custom_df.drop(columns=['date'], errors='ignore')
            if 'date' in removed_custom_df.columns:
                removed_custom_df = removed_custom_df.drop(columns=['date'], errors='ignore')

            # Forcefully drop the last column if 'date' persists
            custom_df = custom_df.iloc[:, :-1] if 'date' in custom_df.columns else custom_df
            core_df = core_df.iloc[:, :-1] if 'date' in core_df.columns else core_df
            filtered_custom_df = filtered_custom_df.iloc[:, :-1] if 'date' in filtered_custom_df.columns else filtered_custom_df
            removed_custom_df = removed_custom_df.iloc[:, :-1] if 'date' in removed_custom_df.columns else removed_custom_df

            # Debug: Check if 'date' column is removed
            print("After dropping 'date':")
            print("custom_df columns:", custom_df.columns)
            print("core_df columns:", core_df.columns)
            print("filtered_custom_df columns:", filtered_custom_df.columns)
            print("removed_custom_df columns:", removed_custom_df.columns)

            # Save the original custom survey data
            original_custom_file_name = construct_file_name("Custom Removed", core_date)
            original_custom_csv_file = os.path.join(custom_save_directory, f"{original_custom_file_name}.dat")
            custom_df = custom_df.astype(str)
            custom_df.to_csv(original_custom_csv_file, sep="\t", index=False)
            print(f"Original Custom survey data saved to '{original_custom_csv_file}'")

            # Save the filtered custom survey data
            filtered_custom_file_name = construct_file_name("Custom", core_date)
            filtered_custom_csv_file = os.path.join(custom_save_directory, f"{filtered_custom_file_name}.dat")
            filtered_custom_df = filtered_custom_df.astype(str)
            filtered_custom_df.to_csv(filtered_custom_csv_file, sep="\t", index=False)
            print(f"Filtered Custom survey data saved to '{filtered_custom_csv_file}'")

            # Save the removed custom survey data
            removed_custom_file_name = construct_file_name("Custom Removed", core_date)
            removed_custom_csv_file = os.path.join(custom_save_directory, f"{removed_custom_file_name}.dat")
            removed_custom_df = removed_custom_df.astype(str)
            removed_custom_df.to_csv(removed_custom_csv_file, sep="\t", index=False)
            print(f"Removed Custom survey data saved to '{removed_custom_csv_file}'")

            total_custom = len(custom_df)
            matched_custom = len(filtered_custom_df)
            unmatched_custom = len(removed_custom_df)

            print(f"Total Custom Records: {total_custom}")
            print(f"Matched Custom Records: {matched_custom}")
            print(f"Unmatched Custom Records: {unmatched_custom}")
            print(f"Completion Rate: {matched_custom / total_custom * 100:.2f}%")
except Exception as e:
    print(f"An error occurred: {e}")
