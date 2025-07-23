import pandas as pd
import os
import json

# --- Load base directory from ALL_DIRECTORY.json ---
with open("ALL_DIRECTORY.json", "r") as f:
    dir_data = json.load(f)

base_dir = dir_data["base_directory"]

# --- Prompt for PQ ID ---
question_id = input("Enter the PQ ID (e.g., QP0005069): ").strip()

# --- Define ALL_CORE and ALL_CUSTOM file paths ---
all_core_path = os.path.join(base_dir, "ALL_CORE.dat")
all_custom_path = os.path.join(base_dir, "ALL_CUSTOM.dat")

# --- Function to filter relevant columns ---
def filter_columns(df, question_id):
    base_columns = df.columns[:12].tolist()
    matching_columns = [col for col in df.columns if col.startswith(question_id)]
    return df[base_columns + matching_columns]

# --- Read and filter files if they exist ---
dfs = []

for file_path in [all_core_path, all_custom_path]:
    if os.path.exists(file_path):
        print(f"Processing: {file_path}")
        df = pd.read_csv(file_path, sep="\t", dtype=str)
        filtered_df = filter_columns(df, question_id)
        dfs.append(filtered_df)
    else:
        print(f"⚠️ File not found: {file_path}")

# --- Combine filtered dataframes and export to Excel ---
if dfs:
    combined_df = pd.concat(dfs, ignore_index=True)
    output_filename = f"{question_id}.xls"  # Excel 97-03 format
    output_path = os.path.join(base_dir, output_filename)
    combined_df.to_excel(output_path, index=False)
    print(f"✅ Excel file saved: {output_path}")
else:
    print("❌ No data to process.")
