import os
import pandas as pd
from datetime import datetime
import io
import requests
import json  # Import the json module

# Set up your API key and server domain
api_key = "uqdpv1ehf12fc3bangxb9kh4m7y0wbp5ffrp87qxt5kssvsxcfncfqm3d4z6dvnm"
server_domain = "https://nrc.decipherinc.com/api/v1/"
survey_id = "selfserve/53b/250304"  # Core OS survey

# Define the vendor mapping with numerical IDs
vendor_mapping = {
    1: "SSI",
    7: "Cint",
    6: "Prodege"
}

# Function to download survey data and return as DataFrame
def download_survey_data(survey_id):
    layout = 89  # Hardcoded layout ID
    url = f"{server_domain}surveys/{survey_id}/data"
    headers = {"x-apikey": api_key}
    params = {
        "format": "csv",  # Request data in CSV format
        "layout": layout  # Include layout ID in the request
    }

    response = requests.get(url, headers=headers, params=params)
    if response.status_code == 200:
        data = response.text
        # Convert the CSV data into a DataFrame
        df = pd.read_csv(io.StringIO(data))
        if df.empty:
            return None
        return df
    else:
        return None

# Function to download the datamap
def download_datamap(survey_id):
    layout = 89  # Hardcoded layout ID
    url = f"{server_domain}surveys/{survey_id}/datamap"
    headers = {"x-apikey": api_key}
    params = {
        "format": "json",  # Request datamap in JSON format
        "layout": layout  # Include layout ID in the request
    }

    response = requests.get(url, headers=headers, params=params)
    if response.status_code == 200:
        return response.json()  # Return the datamap as a dictionary
    else:
        return None

# Function to replace values in the DataFrame using the datamap
def replace_values_with_datamap(df, datamap):
    if df is None or datamap is None:
        return df

    if df.empty:
        return df

    for variable in datamap["variables"]:
        column = variable.get("vgroup")  # Use 'vgroup' instead of 'label'
        values = variable.get("values")

        if column in df.columns and values:
            value_mapping = {v["value"]: v["title"] for v in values}
            df[column] = df[column].map(value_mapping).fillna(df[column])

    return df

# Function to save the DataFrame with only vgroup columns as a CSV file
def save_dataframe_to_csv(df, datamap):
    if df is None:
        return

    # Dynamically extract vgroup columns from the datamap
    vgroup_columns = [variable.get("vgroup") for variable in datamap["variables"] if variable.get("vgroup") is not None]

    # Ensure only columns that exist in the DataFrame are retained
    vgroup_columns = [col for col in vgroup_columns if col in df.columns]

    # Exclude columns with '00' in their names
    vgroup_columns = [col for col in vgroup_columns if '00' not in col]

    # Exclude specific columns: 'CintID' and 'QTOM_SystemPreference'
    excluded_columns = {'CintID', 'QTOM_SystemPreference'}
    vgroup_columns = [col for col in vgroup_columns if col not in excluded_columns]

    # Filter the DataFrame to include only vgroup columns
    df = df[vgroup_columns]

    # Filter rows where status = "Qualified"
    if 'status' in df.columns:
        df = df[df['status'] == "Qualified"]

    # Save the filtered DataFrame to a CSV file
    file_path = os.path.join(os.getcwd(), "survey_data.csv")
    df.to_csv(file_path, index=False)

# Function to print all vgroup headers from the datamap
def print_vgroup_headers(datamap):
    if datamap is None:
        return

    vgroup_headers = [variable.get("vgroup") for variable in datamap["variables"] if variable.get("vgroup") is not None]

# Function to create a matrix of total counts per vendor
def create_vendor_matrix(df):
    if df is None or 'QVENDOR' not in df.columns:
        return None

    # Filter rows where status = "Qualified"
    if 'status' in df.columns:
        df = df[df['status'] == "Qualified"]

    # Exclude specific columns: 'CintID', 'QTOM_SystemPreference', and columns with '00' in their names
    excluded_columns = {'CintID', 'QTOM_SystemPreference'}
    columns_after_qvendor = [
        col for col in df.columns[df.columns.get_loc('QVENDOR') + 1:]
        if '00' not in col and col not in excluded_columns
    ]

    # Filter the DataFrame to include only relevant columns
    filtered_df = df[['QVENDOR'] + columns_after_qvendor]

    # Create a pivot table with vendors as columns and other columns as rows
    matrix = filtered_df.groupby('QVENDOR')[columns_after_qvendor].count().transpose()

    # Reorder vendors to the specified order
    vendor_order = ['SSI', 'Prodege', 'Cint']
    matrix = matrix.reindex(columns=vendor_order, fill_value=0)

    # Add grand totals for rows
    matrix['Grand Total'] = matrix.sum(axis=1)  # Add row totals

    # Sort rows alphabetically, except for the 'Grand Total' row
    matrix = matrix.sort_index()
    if 'Grand Total' in matrix.index:
        grand_total = matrix.loc['Grand Total']
        matrix = matrix.drop('Grand Total')
        matrix = pd.concat([matrix, pd.DataFrame(grand_total).transpose()])

    # Save the matrix to a CSV file
    file_path = os.path.join(os.getcwd(), "vendor_matrix.csv")
    matrix.to_csv(file_path)
    print(f"Vendor matrix saved to {file_path}.")

# Download data for the Core survey
df = download_survey_data(survey_id)

# Download the datamap for the survey
datamap = download_datamap(survey_id)

# Print all vgroup headers from the datamap
print_vgroup_headers(datamap)

# Load the datamap directly from the response
if datamap is None:
    pass
else:
    # Replace values in the DataFrame using the datamap
    df = replace_values_with_datamap(df, datamap)

    # Save the DataFrame with only vgroup columns to a CSV file
    save_dataframe_to_csv(df, datamap)

    # Create a matrix of total counts per vendor
    create_vendor_matrix(df)
