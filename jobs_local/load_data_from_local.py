from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Write DataFrame to PostgreSQL") \
    .config("spark.jars", "./jars/postgresql-42.7.3.jar") \
    .getOrCreate()

csv_directory = "/mnt/spark_data_lake/outpu/"

# read from csv
df_group_date = spark.read.csv(f"{csv_directory}/group_date", header=True, inferSchema=True)
df_group_year = spark.read.csv(f"{csv_directory}/group_year", header=True, inferSchema=True)

jdbc_url = "jdbc:postgresql://localhost:5432/mydatabase"
properties = {
    "user": "admin",
    "password": "admin123",
    "driver": "org.postgresql.Driver"
}

# df write to database
df_group_date.write.jdbc(url=jdbc_url, table="drive_data_by_date", mode="append", properties=properties)
df_group_year.write.jdbc(url=jdbc_url, table="drive_data_by_year", mode="append", properties=properties)

spark.stop()
