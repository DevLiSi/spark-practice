import requests
import zipfile
from concurrent.futures import ThreadPoolExecutor

target_folder = "./datalake/original_data"

# download date list
date_array = [
              "Q3_2019", "Q4_2019",
              "Q1_2020", "Q2_2020", "Q3_2020", "Q4_2020",
              "Q1_2021", "Q2_2021", "Q3_2021", "Q4_2021",
              "Q1_2022", "Q2_2022", "Q3_2022", "Q4_2022",
              "Q1_2023", "Q2_2023", "Q3_2023"]


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

print("All data have finished...")
