from pyspark.sql import SparkSession
import pyspark.sql.functions as sf

# 创建 SparkSession
spark = SparkSession.builder \
    .appName("Read All CSV Files") \
    .getOrCreate()

# 定义要读取的目录路径
csv_directory = "./original_data"
output_directory = "./output_data"

# 读取目录下的所有 CSV 文件
df = spark.read.csv(csv_directory, header=True, inferSchema=True)
selected_df = df.select(df.date, df.model, df.failure)

group_date_df = selected_df.groupBy("date") \
    .agg(sf.count("model").alias("count"), sf.sum("failure").alias("πfailure")) \
    .sort(sf.asc("date"))

group_date_df.write.csv(output_directory, header=True, mode="overwrite")

# 打印 DataFrame 的 Schema
group_date_df.printSchema()

row_count = group_date_df.count()
print(f"DataFrame 总行数: {row_count}")

# 停止 SparkSession
spark.stop()
