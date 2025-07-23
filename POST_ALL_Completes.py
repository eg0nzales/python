import os
import pandas as pd
import json
from datetime import datetime

# Load the date info for folder naming
with open("Completes_dates.json", "r") as f:
    time_data = json.load(f)

end_date = datetime(*time_data["end_date"])
month_folder = end_date.strftime("%m.%Y")
year_folder = str(end_date.year)

base_dir = r"T:\MarketInsights\System and Data Files\Data"
target_dir = os.path.join(base_dir, year_folder, month_folder)

# Columns of interest to keep (customize as needed)
columns_of_interest = ["Q1", "Q2", "QZIPCODE", "QFIPS", "starttime", "endtime"]

def get_files_by_pattern(dir_path, include_patterns=[], exclude_patterns=[]):
    all_files = os.listdir(dir_path)
    filtered_files = []
    for f in all_files:
        if all(pat in f for pat in include_patterns) and all(pat not in f for pat in exclude_patterns):
            filtered_files.append(f)
    return filtered_files

def load_and_combine_files(file_list):
    dfs = []
    for f in file_list:
        full_path = os.path.join(target_dir, f)
        print(f"Loading {full_path}")
        df = pd.read_csv(full_path, sep="\t", dtype=str)
        dfs.append(df)
    return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()

# Gather files
core_files = get_files_by_pattern(target_dir, include_patterns=["Core"], exclude_patterns=["OVERSAMPLE", "Custom"])
core_oversample_files = get_files_by_pattern(target_dir, include_patterns=["Core", "OVERSAMPLE"], exclude_patterns=["Custom"])
custom_files = get_files_by_pattern(target_dir, include_patterns=["Custom"], exclude_patterns=["OVERSAMPLE"])
custom_oversample_files = get_files_by_pattern(target_dir, include_patterns=["Custom", "OVERSAMPLE"])

# Load and combine dataframes
core_df = load_and_combine_files(core_files)
core_os_df = load_and_combine_files(core_oversample_files)
custom_df = load_and_combine_files(custom_files)
custom_os_df = load_and_combine_files(custom_oversample_files)

# Combine halves
full_core_df = pd.concat([core_df, core_os_df], ignore_index=True) if not core_df.empty or not core_os_df.empty else pd.DataFrame()
full_custom_df = pd.concat([custom_df, custom_os_df], ignore_index=True) if not custom_df.empty or not custom_os_df.empty else pd.DataFrame()

# Filter columns
def filter_columns(df, columns):
    if not df.empty:
        existing_cols = [col for col in columns if col in df.columns]
        return df[existing_cols]
    return df

full_core_df = filter_columns(full_core_df, columns_of_interest)
full_custom_df = filter_columns(full_custom_df, columns_of_interest)

# Create Combined folder
combined_dir = os.path.join(target_dir, "Combined")
os.makedirs(combined_dir, exist_ok=True)

# Save outputs
def save_df(df, filename):
    if not df.empty:
        path = os.path.join(combined_dir, filename)
        df.to_csv(path, index=False)
        print(f"Saved {filename} ({len(df)} rows)")
    else:
        print(f"No data to save for {filename}")

save_df(full_core_df, f"Combined_Core_{year_folder}_{month_folder}.csv")
save_df(full_custom_df, f"Combined_Custom_{year_folder}_{month_folder}.csv")

print("All combined files saved successfully!")
