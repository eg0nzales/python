import json

with open("survey_ids.json", "r") as f:
    config = json.load(f)

survey_id_1 = config["survey_id_1"]
survey_id_2 = config["survey_id_2"]

print("Core Survey ID:", survey_id_1)
print("Custom Survey ID:", survey_id_2)

import requests
import pandas as pd
from datetime import datetime
from tqdm import tqdm
import io
import unicodedata

# Set up your API key, server domain, and survey IDs
api_key = "uqdpv1ehf12fc3bangxb9kh4m7y0wbp5ffrp87qxt5kssvsxcfncfqm3d4z6dvnm"
server_domain = "https://nrc.decipherinc.com/api/v1/"
start_date = datetime(2025, 1, 2, 0, 0)  # EDIT [YYYY, Month, Date, Hour, Second] January 2, 2025, 00:00 (midnight)
end_date = datetime(2025, 1, 10, 23, 59)  # EDIT [YYYY, M, DD, HH, SS] January 10, 2025, 23:59 (one minute before midnight)

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
                return None, None
         
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
            
            # Convert DataFrame to string type to ensure 'nan' replacement
            filtered_df = filtered_df.astype(str)
            
            # Replace 'nan' text results with a blank
            filtered_df = filtered_df.replace(r'\bnan\b', '', regex=True)
                
            # Ensure 'date' column is datetime before using .dt accessor
            filtered_df['date'] = pd.to_datetime(filtered_df['date'], errors='coerce')
            date = filtered_df['date'].dt.strftime("%B %Y").iloc[0]
            
            # Remove the 'date' column
            filtered_df = filtered_df.drop(columns=['date'])
            
            csv_file_name = f"{date} - {survey_type}.dat"
            # Save the file with utf-8 encoding to ensure compatibility
            filtered_df.to_csv(csv_file_name, sep="\t", index=False, na_rep='', encoding='utf-8')

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

    if core_df is not None and core_csv_file is not None:
        core_record_ids = set(core_df.iloc[:, 0].astype(str))
        
        custom_df, custom_csv_file = download_survey_data(survey_id_2, "Custom")

        if custom_df is not None and custom_csv_file is not None:
            original_custom_csv_file = custom_csv_file.replace("Custom", "Custom Removed")
            custom_df = custom_df.astype(str)
            custom_df.to_csv(original_custom_csv_file, sep="\t", index=False)
            print(f"Original Custom survey data saved to '{original_custom_csv_file}'")

            filtered_custom_df = custom_df[custom_df.iloc[:, 0].astype(str).isin(core_record_ids)]
            removed_custom_df = custom_df[~custom_df.iloc[:, 0].astype(str).isin(core_record_ids)]

            filtered_custom_csv_file = core_csv_file.replace("Core", "Custom")
            filtered_custom_df = filtered_custom_df.astype(str)
            filtered_custom_df.to_csv(filtered_custom_csv_file, sep="\t", index=False)
            print(f"Filtered Custom survey data saved to '{filtered_custom_csv_file}'")

            removed_custom_csv_file = core_csv_file.replace("Core", "Custom Removed")
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
