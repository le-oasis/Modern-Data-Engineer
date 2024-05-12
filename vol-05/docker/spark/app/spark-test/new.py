# spark_app.py

from pyspark.sql import SparkSession
import os



# Get environment variables
user = os.getenv('POSTGRES_USER')
password = os.getenv('POSTGRES_PASSWORD')  # Changed from POSTGRES_USER to POSTGRES_PASSWORD
minio_access_key = os.getenv('MINIO_ACCESS_KEY')
minio_secret_key = os.getenv('MINIO_SECRET_KEY')
minio_url = os.getenv('MINIO_URL')

# Initialize Spark
# Spark session & context
spark = SparkSession.builder \
    .appName("read-postgres") \
    .config("spark.jars.packages", "org.postgresql:postgresql:42.4.0") \
    .config("spark.hadoop.fs.s3a.endpoint", minio_url) \
    .config("spark.hadoop.fs.s3a.access.key", minio_access_key) \
    .config("spark.hadoop.fs.s3a.secret.key", minio_secret_key) \
    .config("spark.hadoop.fs.s3a.path.style.access", True) \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

# Read data from the actor table
df_actor = (
    spark.read
    .format("jdbc")
    .option("url", "jdbc:postgresql://oasispostgresdb:5432/dvdrental")
    .option("dbtable", "public.actor")
    .option("user", "airflow")
    .option("password", "airflow")
    .load()
)

# Transform the data
df_transformed = df_actor.limit(100)

# Write the transformed data to MinIO
df_transformed.write.parquet(f"s3a://{minio_url}/bronze/data.parquet")