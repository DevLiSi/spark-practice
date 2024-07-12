import os
import zipfile
from concurrent.futures import ThreadPoolExecutor
import boto3

import requests

target_folder = "/Users/sili/Bigdata/local_data_lake"

# download date list
date_array = ["Q1_2019", "Q2_2019"]


def download_and_unzip(data_date):
    url = f"https://f001.backblazeb2.com/file/Backblaze-Hard-Drive-Data/data_{data_date}.zip"
    local_zip_path = f"temp_file_{data_date}.zip"
    print(f"start download {data_date}datasource...")
    response = requests.get(url)
    print(f"end download {data_date} datasource...")
    with open(local_zip_path, "wb") as file:
        file.write(response.content)
    # unzip file
    with zipfile.ZipFile(local_zip_path, "r") as zip_ref:
        zip_ref.extractall(target_folder)


with ThreadPoolExecutor(max_workers=2) as executor:
    futures = [executor.submit(download_and_unzip, date) for date in date_array]

    for future in futures:
        future.result()

# 配置AWS访问
aws_access_key_id = os.getenv('AWS_ACCESS_KEY_ID')
aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY')
aws_session_token = os.getenv('AWS_SESSION_TOKEN')

s3 = boto3.client('s3', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key,
                  aws_session_token=aws_session_token, region_name='eu-north-1')
bucket_name = 'sili-spark-test'

for root, dirs, files in os.walk(target_folder):
    for file in files:
        if file.endswith('.csv'):
            file_path = os.path.join(root, file)
            s3_key = os.path.relpath(file_path, target_folder)

            try:
                s3.upload_file(file_path, bucket_name, s3_key)
                print(f"Uploaded {file_path} to s3://{bucket_name}/{s3_key}")
            except Exception as e:
                print(f"Failed to upload {file_path}: {e}")

print("All data have finished...")
