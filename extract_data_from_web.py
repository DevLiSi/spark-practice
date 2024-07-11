import os
import requests
import zipfile
import pandas as pd

# 定义下载 URL 和本地文件路径
url = "https://f001.backblazeb2.com/file/Backblaze-Hard-Drive-Data/data_Q1_2019.zip"
local_zip_path = "temp_file.zip"
extracted_folder = "source_data"

# 下载 ZIP 文件
print("start download datasource...")
response = requests.get(url)
print("end download datasource...")

with open(local_zip_path, "wb") as file:
    file.write(response.content)

# 解压缩 ZIP 文件
with zipfile.ZipFile(local_zip_path, "r") as zip_ref:
    zip_ref.extractall(extracted_folder)

# 遍历解压后的目录，读取所有 CSV 文件
csv_files = [os.path.join(extracted_folder, file) for file in os.listdir(extracted_folder) if file.endswith('.csv')]

# 初始化一个空的 DataFrame 列表
dfs = []

# 读取所有 CSV 文件并存储到 DataFrame 列表中
for csv_file in csv_files:
    df = pd.read_csv(csv_file)
    dfs.append(df)

# 如果你想合并所有的 DataFrame，可以使用 pd.concat
combined_df = pd.concat(dfs, ignore_index=True)

# 显示合并后的 DataFrame 内容
print(combined_df.head())