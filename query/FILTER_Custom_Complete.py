import pandas as pd
import os
import json

# --- Load base directory from ALL_DIRECTORY.json ---
with open("ALL_DIRECTORY.json", "r") as f:
    dir_data = json.load(f)

base_dir = dir_data["base_directory"]

# --- Prompt for PQ IDs (comma separated) ---
pq_ids_str = input("Enter PQ IDs (comma separated, e.g., Q0050069,QP0009620): ").strip()
pq_ids = [pq.strip() for pq in pq_ids_str.split(",") if pq.strip()]

# --- Define ALL_CORE and ALL_CUSTOM file paths ---
all_core_path = os.path.join(base_dir, "ALL_CORE.dat")
all_custom_path = os.path.join(base_dir, "ALL_CUSTOM.dat")

# --- Function to filter relevant columns ---
def filter_columns(df, question_id):
    base_columns = df.columns[:12].tolist()
    matching_columns = [col for col in df.columns if col.startswith(question_id)]
    return df[base_columns + matching_columns]

# --- Process each PQ ID ---
for question_id in pq_ids:
    dfs = []
    for file_path in [all_core_path, all_custom_path]:
        if os.path.exists(file_path):
            print(f"Processing: {file_path} for PQ ID: {question_id}")
            df = pd.read_csv(file_path, sep="\t", dtype=str)
            filtered_df = filter_columns(df, question_id)
            dfs.append(filtered_df)
        else:
            print(f"⚠️ File not found: {file_path}")
    if dfs:
        combined_df = pd.concat(dfs, ignore_index=True)
        output_filename = f"{question_id}.dat"  # Save as tab-separated .dat file
        output_path = os.path.join(base_dir, output_filename)
        combined_df.to_csv(output_path, sep="\t", index=False)
        print(f"✅ File saved: {output_path}")
    else:
        print(f"❌ No data to process for PQ ID {question_id}")
