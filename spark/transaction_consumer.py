from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_json, struct
from pyspark.sql.types import StructType, StringType, IntegerType, FloatType, BooleanType

# Start SparkSession
spark = SparkSession.builder \
    .appName("FraudDetectionConsumer") \
    .master("local[*]") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1,org.postgresql:postgresql:42.5.0") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Define schema for transaction JSON
schema = StructType() \
    .add("user_id", IntegerType()) \
    .add("amount", FloatType()) \
    .add("location", StringType()) \
    .add("timestamp", StringType()) \
    .add("is_foreign", BooleanType())

# Read from 'transactions' topic
df_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "transactions") \
    .option("startingOffsets", "latest") \
    .load()

# Parse Kafka messages (JSON)
df_parsed = df_raw.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# Detect fraud (foreign & amount > 1000)
df_alerts = df_parsed.filter((col("amount") > 1000) & (col("is_foreign") == True))

# Write fraud alerts to another Kafka topic
alerts_query = df_alerts \
    .select(to_json(struct("*")).alias("value")) \
    .writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("topic", "fraud_alerts") \
    .option("checkpointLocation", "checkpoints/kafka-alerts") \
    .start()

# Store all transactions to PostgreSQL
postgres_query = df_parsed \
    .writeStream \
    .foreachBatch(lambda batch_df, _: batch_df.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://localhost:5432/frauddb") \
        .option("dbtable", "transactions") \
        .option("user", "frauduser") \
        .option("password", "fraudpass") \
        .option("driver", "org.postgresql.Driver") \
        .mode("append") \
        .save()) \
    .outputMode("append") \
    .option("checkpointLocation", "checkpoints/postgres") \
    .start()

# Wait for both streams
alerts_query.awaitTermination()
postgres_query.awaitTermination()
