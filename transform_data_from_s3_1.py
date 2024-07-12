from pyspark.sql import SparkSession
import pyspark.sql.functions as sf
import os


aws_access_key_id = os.getenv('AWS_ACCESS_KEY_ID')
aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY')
aws_session_token = os.getenv('AWS_SESSION_TOKEN')


# 创建 SparkSession
spark = SparkSession.builder \
    .appName("ReadCSVFromS3") \
    .config("spark.hadoop.fs.s3a.access.key", aws_access_key_id) \
    .config("spark.hadoop.fs.s3a.secret.key", aws_secret_access_key) \
    .config("spark.hadoop.fs.s3a.session.token", aws_session_token) \
    .config("spark.hadoop.fs.s3a.endpoint", "s3.eu-north-1.amazonaws.com") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("com.amazonaws.services.s3.enableV4", "true") \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.1") \
    .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2") \
    .getOrCreate()

bucket_name = 'sili-spark-test'


# 读取目录下的所有 CSV 文件
df = spark.read.csv(f"s3a://{bucket_name}/2019-01-08.csv", header=True, inferSchema=True)
selected_df = df.select(df.date, df.model, df.failure)

group_date_df = selected_df.groupBy("date") \
    .agg(sf.count("model").alias("count"), sf.sum("failure").alias("failure")) \
    .sort(sf.asc("date"))


group_date_df.printSchema()

row_count = group_date_df.count()
print(f"DataFrame 总行数: {row_count}")

s3_path = f"s3a://{bucket_name}/group_date_dws"

# write to S3
group_date_df.write \
  .format("csv") \
  .mode("overwrite") \
  .option("header", "true") \
  .save(s3_path)

# 停止 SparkSession
spark.stop()
