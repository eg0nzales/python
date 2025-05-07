import json
import os
import requests
import pandas as pd
from datetime import datetime

try:
    with open("Completes_ids.json", "r") as f:
        config = json.load(f)

    # Check if the JSON file exists
    directory_data_path = "Directory_Data.json"
    if not os.path.exists(directory_data_path):
        raise FileNotFoundError(f"Required file not found: {directory_data_path}")

    with open(directory_data_path, "r") as f:
        raw_data = f.read()
        print(f"Raw JSON data from Directory_Data.json:\n{raw_data}")
        directory_data = json.loads(raw_data)  # Parse the JSON data
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

# Set up your API key, server domain, and survey IDs
api_key = "uqdpv1ehf12fc3bangxb9kh4m7y0wbp5ffrp87qxt5kssvsxcfncfqm3d4z6dvnm"
server_domain = "nrc.decipherinc.com"  # Corrected to only include the domain name

# Function to download survey layout and save as CSV
def download_survey_layout(survey_id, layout_id, survey_type):
    # Corrected URL construction
    url = f"https://{server_domain}/api/v1/surveys/{survey_id}/layouts/{layout_id}"
    headers = {"x-apikey": api_key}

    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        layout_data = response.json()
        try:
            # Extract variables from layout data
            variables = layout_data.get("variables", [])
            if not variables:
                print(f"No variables found in layout for survey {survey_id}.")
                return None

            # Convert variables to a DataFrame
            df = pd.DataFrame(variables)

            # Filter rows where 'shown' is True
            df = df[df['shown'] == True]

            # Keep only the 'label', 'altlabel', and 'fwidth' columns
            df = df[['label', 'altlabel', 'fwidth']]

            # Add a new column (D) with the specified formula logic
            df['D'] = df.apply(
                lambda row: "" if pd.isna(row['label']) else (
                    "Yes" if (
                        "r" in str(row['altlabel']).lower() and
                        ("QP0" in str(row['altlabel']) or "Q0" in str(row['altlabel']))
                    ) else "No"
                ),
                axis=1
            )

            # Add a new column (E) with the specified formula logic
            df['E'] = df.apply(
                lambda row: "" if pd.isna(row['label']) else (
                    "" if row['D'] == "No" else (
                        "OPS" if "oe" in str(row['altlabel']).lower() else "Response"
                    )
                ),
                axis=1
            )

            # Add a new column (F) with the specified formula logic
            df['F'] = df['label'].apply(lambda x: "" if pd.isna(x) else x)

            # Add a new column (G) with the specified formula logic
            def calculate_column_g(row):
                if pd.isna(row['label']):
                    return ""
                if row['E'] == "Response":
                    if "r" in str(row['altlabel']).lower():
                        return str(row['altlabel']).replace("r", "_", 1)
                elif row['E'] == "OPS":
                    if "r" in str(row['altlabel']).lower() and "oe" in str(row['altlabel']).lower():
                        temp = str(row['altlabel']).replace("r", "o", 1)
                        return temp.replace("oe", "", 1)
                return row['altlabel']

            df['G'] = df.apply(calculate_column_g, axis=1)

            # Add a new column (H) with the specified formula logic
            df['H'] = df['fwidth'].apply(lambda x: "" if pd.isna(x) else x)

            # Add a new column (I) with the specified formula logic
            df['I'] = df.apply(
                lambda row: "" if pd.isna(row['label']) else f"{row['F']},{row['G']},{row['H']}",
                axis=1
            )

            # Keep only column I
            df = df[['I']]

            # Generate file name and directory
            current_date = datetime.now().strftime("%m%d%y")
            year_month = datetime.now().strftime("%Y_%m")
            layout_directory = os.path.join(base_directory, "Layouts")
            if not os.path.exists(layout_directory):
                print(f"Layouts directory does not exist. Creating: {layout_directory}")
                os.makedirs(layout_directory)

            file_name = f"Layout.{year_month}.{survey_type}.TEST{current_date}.csv"
            file_path = os.path.join(layout_directory, file_name)

            # Save only column I to CSV file
            df.to_csv(file_path, index=False, header=False)

            print(f"Layout data downloaded successfully and saved to '{file_path}'")
            return file_path
        except Exception as e:
            print(f"An error occurred while saving layout data for survey {survey_id}: {e}")
            return None
    else:
        print(f"Error downloading layout for survey {survey_id}: {response.status_code}")
        return None

try:
    # Use layout ID 61 for syndicate and 66 for custom
    core_layout_file = download_survey_layout(survey_id_1, 61, "Core")
    custom_layout_file = download_survey_layout(survey_id_2, 66, "Custom")

    if core_layout_file is not None:
        print(f"Core survey layout saved to '{core_layout_file}'")

    if custom_layout_file is not None:
        print(f"Custom survey layout saved to '{custom_layout_file}'")

except Exception as e:
    print(f"An error occurred: {e}")
