import json
import os
import pandas as pd
import requests
from datetime import datetime
from tqdm import tqdm
import io
from time import sleep

# Load config files
with open("Completes_ids.json", "r") as f:
    config = json.load(f)

with open("Directory_Incidence.json", "r") as f:
    directory_incidence = json.load(f)

survey_id_1 = config["survey_id_1"]
survey_id_2 = config["survey_id_2"]
base_folder_path = directory_incidence["base_directory"]

# API Setup
api_key = "uqdpv1ehf12fc3bangxb9kh4m7y0wbp5ffrp87qxt5kssvsxcfncfqm3d4z6dvnm"
server_domain = "https://nrc.decipherinc.com/api/v1/"

vendor_groups = {
    "SSI": [1],
    "Cint": [7, 139],
    "Toluna": [2],
    "Prodege": [6],
    "MyPoints": [3]
}

vendor_mapping = {vendor_id: name for name, ids in vendor_groups.items() for vendor_id in ids}

vendors_folders = {
    "Cint": "Cint",
    "Prodege": "Prodege and MyPoints",
    "SSI": "SSI",
    "Toluna": "Toluna"
}

def download_survey_data(survey_id, survey_type):
    layout = 73 if survey_id == survey_id_1 else 74
    url = f"{server_domain}surveys/{survey_id}/data"
    headers = {"x-apikey": api_key}
    params = {
        "format": "tab",
        "condition": "qualified",
        "layout": layout
    }

    for attempt in range(3):
        try:
            response = requests.get(url, headers=headers, params=params, timeout=10)
            if response.status_code == 200:
                df = pd.read_csv(io.StringIO(response.text), sep="\t", low_memory=False)
                if 'status' not in df.columns:
                    return None

                if 'Survey start time' not in df.columns:
                    return None
                if 'Survey end time' not in df.columns:
                    df['Survey end time'] = df['Survey start time']

                df['Survey start time'] = pd.to_datetime(df['Survey start time'], errors='coerce')
                df['Survey end time'] = pd.to_datetime(df['Survey end time'], errors='coerce')
                df = df.dropna(subset=['Survey start time', 'Survey end time'])

                if survey_type == "Syndicate":
                    df = df.iloc[:, :19]
                else:
                    df = df.iloc[:, :17]

                return df
        except requests.exceptions.RequestException:
            sleep(2)
    return None

def save_files_by_vendor(df, survey_type):
    if df is None:
        return

    for vendor_id in tqdm(df['Vendor'].unique(), desc=f"Saving {survey_type} files"):
        vendor_name = vendor_mapping.get(vendor_id, 'Unknown')
        if vendor_name in ['Unknown', 'MyPoints']:
            continue

        folder_name = vendors_folders.get(vendor_name, 'Unknown')
        file_vendor_name = "Dynata" if vendor_name == "SSI" else vendor_name

        vendor_df = df[df['Vendor'] == vendor_id]

        # 🔹 Apply conditional filter
        if vendor_name != "Cint":
            vendor_df = vendor_df[vendor_df['status'] == 3]

        if vendor_df.empty:
            continue

        start_date = vendor_df['Survey start time'].min()
        month_folder = start_date.strftime("%m.%Y")
        month_for_name = start_date.strftime("%B%y" if survey_type == "Syndicate" else "%B%Y")

        base_path = base_folder_path if survey_type == "Syndicate" else os.path.join(base_folder_path, "Online Oversample")
        folder_path = os.path.join(base_path, folder_name, '2025', month_folder)
        os.makedirs(folder_path, exist_ok=True)

        prefix = "Incidence.OS" if survey_type == "Oversample" else "Incidence"
        file_name = f"{prefix}.{file_vendor_name}.{month_for_name}.csv"
        file_path = os.path.join(folder_path, file_name)

        vendor_df.to_csv(file_path, index=False)

        # Log row count
        print(f"Saved {len(vendor_df)} rows for {vendor_name} ({survey_type}) → {file_path}")

# Process surveys
df1 = download_survey_data(survey_id_1, "Syndicate")
save_files_by_vendor(df1, "Syndicate")

df2 = download_survey_data(survey_id_2, "Oversample")
save_files_by_vendor(df2, "Oversample")
