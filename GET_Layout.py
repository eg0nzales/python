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
except (json.JSONDecodeError, FileNotFoundError) as e:
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

# Function to download survey layout and save only column I as CSV
def download_survey_layout(survey_id, layout_id, survey_type):
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

            # Generate the final column I with corrected replacement logic
            def generate_column_i(row):
                if pd.isna(row['label']):
                    return ""
                altlabel = str(row['altlabel'])
                if "r" in altlabel.lower():
                    if "oe" in altlabel.lower():
                        # Replace "r" with "o" and remove "oe"
                        altlabel = altlabel.replace("r", "o", 1).replace("oe", "", 1)
                    else:
                        # Replace "r" with "_"
                        altlabel = altlabel.replace("r", "_", 1)
                return f"{row['label']},{altlabel},{row['fwidth']}"

            df['I'] = df.apply(generate_column_i, axis=1)

            # Keep only column I
            df = df[['I']]

            # Generate file name and directory
            current_date = datetime.now().strftime("%m%d%y")
            year_month = datetime.now().strftime("%Y_%m")
            layout_directory = os.path.join(base_directory, "Layouts")
            os.makedirs(layout_directory, exist_ok=True)

            file_name = f"Layout.{year_month}.{survey_type}.TEST{current_date}.csv"
            file_path = os.path.join(layout_directory, file_name)

            # Save layout data to CSV file
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
