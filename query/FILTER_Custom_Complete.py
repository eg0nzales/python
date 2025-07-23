import pandas as pd
import os
import datetime
import json

# Load base directory from ALL_DIRECTORY.json
with open("ALL_DIRECTORY.json", "r") as f:
    directory_data = json.load(f)
base_dir = directory_data["base_directory"]

def construct_directory_path(base_dir, date_obj):
    year = date_obj.strftime("%Y")
    month_year = date_obj.strftime("%m.%Y")
    return os.path.join(base_dir, year, month_year, "Combined")

def construct_file_name(survey_type, survey_date):
    year_month = survey_date.strftime("%Y_%m")
    suffix = ""
    if "Oversample" in survey_type:
        suffix = ".Oversample"
        survey_type = survey_type.replace(".Oversample", "")
    return f"Decipher.{year_month}.{survey_type}{suffix}.dat"

def filter_columns(file_path, question_id):
    print(f"Processing: {file_path} for question ID: {question_id}")
    df = pd.read_csv(file_path, sep="\t", dtype=str)
    base_columns = df.columns[:12].tolist()
    matching_columns = [col for col in df.columns if col.startswith(question_id)]
    filtered_df = df[base_columns + matching_columns]

    output_filename = f"{question_id}.xlsx"
    output_path = os.path.join(os.path.dirname(file_path), output_filename)
    filtered_df.to_excel(output_path, index=False)
    print(f"✅ Saved: {output_path}\n")

# --- Prompt for date and PQ IDs (comma separated) ---
input_date_str = input("Enter the survey date (e.g., 07/2025): ").strip()
pq_ids_str = input("Enter PQ IDs (comma separated, e.g., QP0006573,QP0009620): ").strip()

try:
    survey_date = datetime.datetime.strptime(input_date_str, "%m/%Y")
except ValueError:
    print("❌ Invalid date format. Use MM/YYYY.")
    exit()

pq_ids = [pq.strip() for pq in pq_ids_str.split(",") if pq.strip()]

combined_dir = construct_directory_path(base_dir, survey_date)
all_core_path = os.path.join(combined_dir, "ALL_CORE.dat")
all_custom_path = os.path.join(combined_dir, "ALL_CUSTOM.dat")

for pq_id in pq_ids:
    for file_path in [all_core_path, all_custom_path]:
        if os.path.exists(file_path):
            filter_columns(file_path, pq_id)
        else:
            print(f"⚠️ File not found: {file_path}")
