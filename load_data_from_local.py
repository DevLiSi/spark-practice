from pyspark.sql import SparkSession

# 创建 SparkSession
spark = SparkSession.builder \
    .appName("Write DataFrame to PostgreSQL") \
    .config("spark.jars", "./jars/postgresql-42.7.3.jar") \
    .getOrCreate()

# 定义要读取的目录路径
csv_directory = "./output_data"

# 读取目录下的所有 CSV 文件
df = spark.read.csv(csv_directory, header=True, inferSchema=True)

# PostgreSQL 配置
jdbc_url = "jdbc:postgresql://localhost:5432/mydatabase"
properties = {
    "user": "admin",
    "password": "admin123",
    "driver": "org.postgresql.Driver"
}

# 将 DataFrame 写入到 PostgreSQL 表中
df.write.jdbc(url=jdbc_url, table="drive_data_by_date", mode="append", properties=properties)

# 停止 SparkSession
spark.stop()