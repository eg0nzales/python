import os
import json
import pandas as pd
import io
import requests
from datetime import datetime, timedelta

# Load configuration from JSON files
with open("Directory_Incidence.json", "r") as f:
    directory_incidence = json.load(f)

with open("Incidence_ids.json", "r") as f:
    surveys = json.load(f)  # Load survey IDs from Incidence_ids.json

base_folder_path = directory_incidence["base_directory"]  # Load base directory from Directory_Incidence.json

# Set up your API key and server domain
api_key = "uqdpv1ehf12fc3bangxb9kh4m7y0wbp5ffrp87qxt5kssvsxcfncfqm3d4z6dvnm"
server_domain = "https://nrc.decipherinc.com/api/v1/"

# Define the vendor mapping with numerical IDs
vendor_mapping = {
    1: "SSI",
    2: "Toluna",
    3: "MyPoints",
    5: "Toluna RTS Memphis",
    6: "Prodege",
    7: "Cint"
}

# Define status lookup table
status_lookup = {
    1: "Terminated",
    2: "Overquota",
    3: "Qualified",
    4: "Partial"
}

# Function to calculate the previous month in MM.YYYY format
def get_previous_month():
    today = datetime.today()
    first_day_of_current_month = today.replace(day=1)
    last_day_of_previous_month = first_day_of_current_month - timedelta(days=1)
    return last_day_of_previous_month.strftime("%m.%Y")

# Function to download survey data and return as DataFrame
def download_survey_data(survey_id):
    print(f"Starting download for survey {survey_id}...")
    url = f"{server_domain}surveys/{survey_id}/data?format=csv"
    headers = {"x-apikey": api_key}

    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        data = io.StringIO(response.text)
        print("Survey data downloaded successfully.")
        df = pd.read_csv(data, low_memory=False)  
        print(f"Downloaded {len(df)} records.")
        return df
    else:
        print(f"Failed to download survey data: {response.status_code}")
        return None

# Function to process the survey data and create a matrix
def create_status_matrix(df, vendor_mapping, status_lookup):
    # Map QVENDOR to vendor names
    df['vendor'] = df['QVENDOR'].map(vendor_mapping)
    
    # Map status to status names
    df['status'] = df['status'].map(status_lookup)
    
    # Create a pivot table with the status as rows and vendors as columns
    matrix = df.pivot_table(index='status', columns='vendor', aggfunc='size', fill_value=0)
    
    return matrix

# Function to save the matrix to a CSV file
def save_matrix(matrix, survey_type):
    # Generate the file name with the previous month
    previous_month = get_previous_month()
    filename = f"{previous_month}_status_vendor_matrix.csv"

    # Determine the output path based on survey type
    if survey_type == "Syndicate":
        output_path = os.path.join(base_folder_path, "Performance Tracker", "Syndicate Matrix", filename)
    elif survey_type == "Oversample":
        output_path = os.path.join(base_folder_path, "Performance Tracker", "OS Matrix", filename)

    # Save the matrix to a CSV file
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    matrix.to_csv(output_path)
    print(f"Matrix saved to {output_path}")

# Main script
if __name__ == "__main__":
    for survey_type, survey_id in surveys.items():
        # Download the survey data
        df = download_survey_data(survey_id)
        
        if df is not None:
            # Create the status matrix
            matrix = create_status_matrix(df, vendor_mapping, status_lookup)
            
            # Display the matrix
            print(f"Matrix for {survey_type}:\n", matrix)
            
            # Save the matrix to a CSV file
            save_matrix(matrix, survey_type)
