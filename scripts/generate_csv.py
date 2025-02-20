import datetime
import os
from google.cloud import storage
import pandas as pd

bucket_name = "ny-taxi-449605-csv-bucket"
# csv_file = "run_counter.csv"
input_csv = "/opt/airflow/dags/scripts/file.csv"
output_csv = "run_counter.csv"

def counter(i, **kwargs):
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    
    # Download input CSV
    # input_blob = bucket.blob(input_csv)
    # input_blob.download_to_filename(input_csv)
    df = pd.read_csv(input_csv, header=None)  # Assuming no header
    
    # Split data into chunks of three
    # grouped_values = [df.iloc[i:i+3, 0].tolist() for i in range(0, len(df), 3)]
    grouped_values = [df.iloc[i*3:(i+1)*3, 0].tolist()]
    
    # Download existing run_counter.csv if exists
    output_blob = bucket.blob(output_csv)
    if output_blob.exists():
        output_blob.download_to_filename(output_csv)
        existing_data = pd.read_csv(output_csv)
    else:
        existing_data = pd.DataFrame(columns=["col1", "timestamp"])
    
    # Prepare new data
    new_data = pd.DataFrame({
        "col1": [','.join(map(str, group)) for group in grouped_values],
        "timestamp": [datetime.datetime.now().isoformat()] * len(grouped_values)
    })
    
    # Merge and save data
    updated_data = pd.concat([existing_data, new_data])
    updated_data.to_csv(output_csv, index=False)
    
    # Upload to GCS
    output_blob.upload_from_filename(output_csv)
    print("Successfully updated run_counter.csv")

# def counter():
#     storage_client = storage.Client()
#     bucket = storage_client.bucket(bucket_name)
#     blob = bucket.blob(csv_file)

#     # Download existing CSV if exists
#     if blob.exists():
#         blob.download_to_filename(csv_file)
#         df = pd.read_csv(csv_file)
#         run_count = df['run_count'].iloc[-1] + 1
#     else:
#         run_count = 1

#     # Create new entry
#     new_data = pd.DataFrame({
#         'timestamp': [datetime.datetime.now().isoformat()],
#         'run_count': [run_count]
#     })

#     # Merge and save data
#     if os.path.exists(csv_file):
#         existing_data = pd.read_csv(csv_file)
#         updated_data = pd.concat([existing_data, new_data])
#     else:
#         updated_data = new_data

#     updated_data.to_csv(csv_file, index=False)
    
#     # Upload to GCS
#     blob.upload_from_filename(csv_file)
#     print(f"Successfully updated CSV with run count: {run_count}")

# Rewrite the code below to read the csv file 'file.csv' as a parameter for counter() (contain one column with numbers), take the three values and put these values later in 'run_counter.csv'. 
# Both files stored in google storage.
# For example, file.csv contain one column with the values 22, 322, 443, 41, 65, 96, 77, 38, 49, 10, 11, 12 in rows.
# File run_counter.csv, should contain 
# col1, col2
# 22,322,443 - timestamp
# 41,65,96 - timestamp
# 77,38,49 - timestamp
# 10,11,12 - timestamp