import json

try:
    with open("Completes_ids.json", "r") as f:
        config = json.load(f)

    with open("Directory_Data.json", "r") as f:
        directory_data = json.load(f)
        print(f"Loaded base directory: {directory_data['base_directory']}")
except json.JSONDecodeError as e:
    print(f"Error reading JSON file: {e}")
    exit(1)
except FileNotFoundError as e:
    print(f"Error: {e}")
    exit(1)


survey_id_1 = config["survey_id_1"]
survey_id_2 = config["survey_id_2"]
base_directory = directory_data["base_directory"]

print("Core Survey ID:", survey_id_1)
print("Custom Survey ID:", survey_id_2)
print("Base Directory:", base_directory)

import requests
import pandas as pd
from datetime import datetime
from tqdm import tqdm
import io
import os

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
        data = response.text
        try:
            df = pd.read_csv(io.StringIO(data), sep="\t", low_memory=False)
            if 'endtime' not in df.columns:
                raise KeyError("'endtime' column is missing in the data.")

            df['date'] = pd.to_datetime(df['endtime'], errors='coerce')
            filtered_df = df.loc[df['status'] == 3].copy()
 
            if filtered_df.empty:
                print(f"No qualified data found for survey {survey_id}.")
                return None, None
            
            # Filter by the first day of data
            first_day = filtered_df['date'].dt.date.min()
            filtered_df = filtered_df[filtered_df['date'].dt.date == first_day]
         
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
                
            # Convert DataFrame to string type to ensure 'nan' replacement
            filtered_df = filtered_df.astype(str)
            
            # Replace 'nan' text results with a blank
            filtered_df = filtered_df.replace(r'\bnan\b', '', regex=True)
                
            # Ensure 'date' column is datetime before using .dt accessor
            filtered_df['date'] = pd.to_datetime(filtered_df['date'], errors='coerce')
            date = filtered_df['date'].dt.strftime("%B %Y").iloc[0]
            year_month = filtered_df['date'].dt.strftime("%Y_%m").iloc[0]
            test_date = filtered_df['date'].dt.strftime("%m%d%y").iloc[0]
            
            # Create directory path based on date
            year = filtered_df['date'].dt.year.iloc[0]
            month_year = filtered_df['date'].dt.strftime("%m.%Y").iloc[0]
            if not os.path.exists(base_directory):
                print(f"Base directory does not exist. Creating: {base_directory}")
                os.makedirs(base_directory)
            
            # Check if TEST folder exists, if not, create it
            test_directory = os.path.join(base_directory, "TEST")
            if not os.path.exists(test_directory):
                print(f"TEST directory does not exist. Creating: {test_directory}")
                os.makedirs(test_directory)
            
            # Remove the 'date' column
            filtered_df = filtered_df.drop(columns=['date'])
            
            # Truncate DataFrame to 30 records before saving
            filtered_df = filtered_df.head(30)
            
            csv_file_name = f"{test_directory}/Decipher.{year_month}.{survey_type}.TEST{test_date}.dat"
            filtered_df.to_csv(csv_file_name, sep="\t", index=False, na_rep='')

            print(f"Qualified data downloaded successfully and saved to '{csv_file_name}'")
            return filtered_df, csv_file_name
        except Exception as e:
            print(f"An error occurred while processing data for survey {survey_id}: {e}")
            return None, None
    else:
        print(f"Error downloading data for survey {survey_id}: {response.status_code}")
        return None, None

try:
    core_df, core_csv_file = download_survey_data(survey_id_1, "Core")
    custom_df, custom_csv_file = download_survey_data(survey_id_2, "Custom")

    if core_df is not None and core_csv_file is not None:
        print(f"Core survey data saved to '{core_csv_file}'")

    if custom_df is not None and custom_csv_file is not None:
        print(f"Custom survey data saved to '{custom_csv_file}'")

except Exception as e:
    print(f"An error occurred: {e}")
