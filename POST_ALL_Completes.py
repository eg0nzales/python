import json
from datetime import datetime, date
import os
import requests
import pandas as pd
import io
import unicodedata

# Load survey IDs
with open("All_Completes_ids.json", "r") as f:
    config = json.load(f)

with open("Directory_Data.json", "r") as f:
    directory_data = json.load(f)

survey_id_1 = config["survey_id_1"]  # Core
survey_id_2 = config["survey_id_2"]  # Core Oversample
survey_id_3 = config["survey_id_3"]  # Custom
survey_id_4 = config["survey_id_4"]  # Custom Oversample
base_directory = directory_data["base_directory"]

server_domain = "https://nrc.decipherinc.com/api/v1/"
api_key = "uqdpv1ehf12fc3bangxb9kh4m7y0wbp5ffrp87qxt5kssvsxcfncfqm3d4z6dvnm"

def format_datetime(value):
    if pd.notnull(value):
        try:
            dt = pd.to_datetime(value, format='%m/%d/%Y %H:%M', errors='coerce')
            return dt.strftime('%-m/%d/%Y %H:%M:00')
        except Exception:
            return value
    return value

def clean_and_normalize_text(value):
    if isinstance(value, str):
        value = value.replace("â€™", "'")
        value = unicodedata.normalize('NFC', value)
    return value

def construct_directory_path(base_dir, date_obj):
    year = date_obj.strftime("%Y")
    month_year = date_obj.strftime("%m.%Y")
    combined_path = os.path.join(base_dir, year, month_year, "Combined")
    return combined_path

def construct_file_name(survey_type, survey_date):
    year_month = survey_date.strftime("%Y_%m")
    suffix = ""
    if "Oversample" in survey_type:
        suffix = ".Oversample"
        survey_type = survey_type.replace(".Oversample", "")
    return f"Decipher.{year_month}.{survey_type}{suffix}.dat"

def download_survey_data(survey_id, survey_type):
    layout = 61 if "Core" in survey_type else 66
    url = f"{server_domain}surveys/{survey_id}/data"
    headers = {"x-apikey": api_key}
    params = {
        "format": "tab",
        "condition": "qualified",
        "layout": layout
    }
    response = requests.get(url, headers=headers, params=params)
    if response.status_code != 200:
        print(f"Error downloading data for survey {survey_id}: {response.status_code}")
        return None, None, None

    data = response.content.decode('utf-8')
    try:
        df = pd.read_csv(io.StringIO(data), sep="\t", low_memory=False)
        if 'endtime' not in df.columns:
            raise KeyError("'endtime' column missing")

        df = df[df['status'] == 3].copy()

        for col in df.columns[4:]:
            if pd.api.types.is_numeric_dtype(df[col]) and col not in ['QZIPCODE', 'QFIPS']:
                df[col] = df[col].apply(lambda x: '{:.0f}'.format(x) if pd.notnull(x) else x)

        for col in ['QZIPCODE', 'QFIPS']:
            if col in df.columns:
                df[col] = df[col].apply(
                    lambda x: f"{int(float(x)):05d}" if pd.notnull(x) and str(x).replace('.', '', 1).isdigit() else x
                )

        for col in ['starttime', 'endtime']:
            if col in df.columns:
                df[col] = df[col].apply(format_datetime)

        if "Custom" in survey_type and 'URI' in df.columns:
            df['URI'] = df['URI'].str[:60]

        for col in df.columns:
            df[col] = df[col].map(clean_and_normalize_text)

        if 'Q0009017T' in df.columns:
            df['Q0009017T'] = df['Q0009017T'].str[:90]

        df = df.astype(str).replace(r'\bnan\b', '', regex=True)

        file_date = pd.to_datetime(df['endtime'].iloc[0])
        save_dir = construct_directory_path(base_directory, file_date)
        os.makedirs(save_dir, exist_ok=True)

        file_name = construct_file_name(survey_type, file_date)
        file_path = os.path.join(save_dir, file_name)

        df.to_csv(file_path, sep="\t", index=False, na_rep='', encoding='utf-8')

        print(f"{survey_type} data saved to {file_path}")
        return df, file_path, save_dir

    except Exception as e:
        print(f"Error processing survey {survey_id}: {e}")
        return None, None, None

def append_files(file_list, output_file):
    dfs = []
    for f in file_list:
        if os.path.exists(f):
            dfs.append(pd.read_csv(f, sep="\t", low_memory=False, dtype=str))
        else:
            print(f"File not found for appending: {f}")
    if dfs:
        combined_df = pd.concat(dfs, ignore_index=True)
        combined_df.to_csv(output_file, sep="\t", index=False, na_rep='', encoding='utf-8')
        print(f"Appended file created at {output_file}")
    else:
        print("No files to append.")

def main():
    downloads = [
        (survey_id_1, "Core"),
        (survey_id_2, "Core.Oversample"),
        (survey_id_3, "Custom"),
        (survey_id_4, "Custom.Oversample")
    ]

    saved_files = {}
    save_dir = None

    # Download Core to determine file_date/save_dir
    df_probe, _, _ = download_survey_data(downloads[0][0], downloads[0][1])
    if df_probe is not None:
        file_date = pd.to_datetime(df_probe['endtime'].iloc[0])
        save_dir = construct_directory_path(base_directory, file_date)
        all_core_path = os.path.join(save_dir, "ALL_CORE.dat")
        all_custom_path = os.path.join(save_dir, "ALL_CUSTOM.dat")

        if os.path.exists(all_core_path) and os.path.exists(all_custom_path):
            print("✅ Skipping download: ALL_CORE.dat and ALL_CUSTOM.dat already exist.")
            
            # Still regenerate ALL_DIRECTORY.json
            all_completes_ids_path = os.path.abspath("All_Completes_ids.json")
            all_completes_dir = os.path.dirname(all_completes_ids_path)
            all_directory_json_path = os.path.join(all_completes_dir, "ALL_DIRECTORY.json")
            all_directory_content = {
                "base_directory": save_dir.replace("/", "\\")
            }
            with open(all_directory_json_path, "w") as jf:
                json.dump(all_directory_content, jf, indent=2)
            print(f"ALL_DIRECTORY.json saved at {all_directory_json_path} with:")
            print(all_directory_content)
            
            return

    else:
        print("❌ Could not determine file_date from Core dataset. Exiting.")
        return

    # Redownload everything now that save_dir is known
    for survey_id, survey_type in downloads:
        df, path, _ = download_survey_data(survey_id, survey_type)
        saved_files[survey_type] = path

    core_files = [saved_files.get("Core"), saved_files.get("Core.Oversample")]
    custom_files = [saved_files.get("Custom"), saved_files.get("Custom.Oversample")]

    if all(core_files):
        append_files(core_files, all_core_path)
    else:
        print("Core files missing, skipping ALL_CORE.dat creation")

    if all(custom_files):
        append_files(custom_files, all_custom_path)
    else:
        print("Custom files missing, skipping ALL_CUSTOM.dat creation")

    # Save ALL_DIRECTORY.json
    all_completes_ids_path = os.path.abspath("All_Completes_ids.json")
    all_completes_dir = os.path.dirname(all_completes_ids_path)
    sample_save_dir = save_dir if save_dir else base_directory
    all_directory_json_path = os.path.join(all_completes_dir, "ALL_DIRECTORY.json")
    all_directory_content = {
        "base_directory": sample_save_dir.replace("/", "\\")
    }
    with open(all_directory_json_path, "w") as jf:
        json.dump(all_directory_content, jf, indent=2)
    print(f"ALL_DIRECTORY.json saved at {all_directory_json_path} with:")
    print(all_directory_content)

if __name__ == "__main__":
    main()
