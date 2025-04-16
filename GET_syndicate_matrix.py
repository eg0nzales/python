import os
import pandas as pd
import io
import requests

# Set up your API key and server domain
api_key = "uqdpv1ehf12fc3bangxb9kh4m7y0wbp5ffrp87qxt5kssvsxcfncfqm3d4z6dvnm"
server_domain = "https://nrc.decipherinc.com/api/v1/"
survey_id = "selfserve/53b/250104"  # Update Core survey id

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
    
    # Reorder the columns to have Prodege, SSI, and Cint
    #matrix = matrix[['MyPoints','Prodege','SSI', 'Toluna','Cint']]
    
    return matrix

# Function to save the matrix to a CSV file
def save_matrix(matrix, filename):
    output_path = os.path.join(r"T:\MarketInsights\HCMG2008\Kinesis\Incidence Reports\2. Performance Tracker\Syndicate Matrix", filename)
    matrix.to_csv(output_path)
    print(f"Matrix saved to {output_path}")

# Main script
if __name__ == "__main__":
    # Download the survey data
    df = download_survey_data(survey_id)
    
    if df is not None:
        # Create the status matrix
        matrix = create_status_matrix(df, vendor_mapping, status_lookup)
        
        # Display the matrix
        print(matrix)
        
        # Save the matrix to a CSV file
        save_matrix(matrix, "02.2025_status_vendor_matrix.csv") #Update File Name
