from pyspark.sql import SparkSession
import pyspark.sql.functions as sf

spark = SparkSession.builder \
    .appName("Read csv files from local data") \
    .getOrCreate()

# 定义要读取的目录路径
csv_directory = "/mnt/spark_data_lake"
output_directory = "/mnt/spark_data_lake/output"

# 读取目录下的所有 CSV 文件
df = spark.read.csv(csv_directory, header=True, inferSchema=True)
selected_df = df.select(df.date, df.model, df.failure)

group_date_df = selected_df.groupBy("date") \
    .agg(sf.count("model").alias("count"), sf.sum("failure").alias("failure")) \
    .sort(sf.asc("date"))

brand = sf.when(selected_df["model"].startswith("CT"), "Crucial") \
    .when(selected_df["model"].startswith("DELLBOSS"), "Dell BOSS ") \
    .when(selected_df["model"].startswith("HGST"), "HGST") \
    .when(selected_df["model"].startswith("ST") | selected_df["model"].startswith("Seagate"), "Seagate") \
    .when(selected_df["model"].startswith("TOSHIBA"), "Toshiba") \
    .when(selected_df["model"].startswith("WDC"), "Western Digital") \
    .otherwise("Other")

selected_df_with_brand = selected_df.withColumn("brand", brand)

group_year_df = selected_df_with_brand.groupby(sf.year("date").alias("year"), "brand") \
    .agg(sf.sum("failure").alias("failure")) \
    .sort(sf.asc("year"))

group_date_df.write.format("csv").mode("overwrite").option("header", "true").save(f"{output_directory}/group_date")
group_year_df.write.format("csv").mode("overwrite").option("header", "true").save(f"{output_directory}/group_year")

group_date_df.printSchema()
group_year_df.printSchema()

row_count = group_date_df.count()
print(f"group_date_df 总行数: {row_count}")
row_count = group_year_df.count()
print(f"group_year_df 总行数: {row_count}")

# 停止 SparkSession
spark.stop()
