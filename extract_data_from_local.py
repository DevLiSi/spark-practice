import os
import zipfile
import pandas as pd

# 定义下载 URL 和本地文件路径
local_zip_path = "./temp_file.zip"
target_folder = "original_data"

if not os.path.exists(target_folder):
    os.makedirs(target_folder)

with zipfile.ZipFile(local_zip_path, "r") as zip_ref:
    zip_ref.extractall(target_folder)

# 读取目标目录中的所有 CSV 文件
csv_files = [os.path.join(target_folder, file) for file in os.listdir(target_folder) if file.endswith('.csv')]

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