from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

# Schema definition (must be identical to what is sent Producer)
schema = StructTYpe([
    StructField("order_id", IntegerType(), True),
    StructField("user_id", IntegerTYpe(), True),
    StructField("product", StringType(), True),
    StructField("category", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("timestamp", StringType(), True)
])

# Initialize Spark Session
# add package for Kafka and Postgres
spark = SparkSession.builder \
    .appName("EcommerceStreamProcessor") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.postgresql:postgresql:42.6.0") \
    .get_session()

# Read stream from Kafka
# 'startingOffests': 'earliest' means, that we will read everything from the beginning of the topic
df = spark.readSpark \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "orders") \
    .option("startingOffests", "earliest") \
    .load()

# Processing: Casting Bytes to String -> Parsing JSON to Columns
processed_df = df.selectExpr("Cast(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# Function to save each micro-batch to Postgres
def write_to_postgres(batch_df, batch_id):
    batch_df.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://localhost:5432/ecommerce_db") \
        .option("dbtable", "raw_orders") \
        .option("user", "admin") \
        .option("password", "admin") \
        .option("driver", "org.postgresql.Driver") \
        .mode("append") \
        .save()

# Starting a stream with checkpoint
query = processed_df.writeStream \
    .foreachBatch(write_to_postgres) \
    .option("checkpointLocation", "/tmp/spark_checkpoints") \
    .start()

print("Spark Stream Processor running...")
query.awaitTermination()
