from src.utils import SPARK_ENV, get_spark_context
from pyspark.sql.functions import from_json, col, avg
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, StringType

if __name__ == "__main__":
    # Create a Spark session and context
    spark = get_spark_context(app_name="Kafka Streaming", config=SPARK_ENV.K8S)
    sc = spark.sparkContext

    kafka_options = {
        "kafka.bootstrap.servers": "kafka:9092",
        "startingOffsets": "earliest",  # Start from the beginning when we consume from kafka
        "subscribe": "INGESTION",  # Our topic name
    }

    df = spark.readStream.format("kafka").options(**kafka_options).load()
    deserialized_df = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

    # TODO - create your logic
    # streaming_df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
    # Define schema for the payload JSON
    payload_schema = StructType([
        StructField("sensor_id", IntegerType()),
        StructField("modality", DoubleType()),
        StructField("unit", StringType()),
        StructField("temporal_aspect", StringType())
    ])
    schema = StructType([
        StructField("payload", StringType()),
        StructField("correlation_id", StringType()),
        StructField("created_at", DoubleType()),
        StructField("schema_version", IntegerType())
    ])

    # Parse the outer JSON
    parsed = deserialized_df.select(from_json(col("value"), schema).alias("data"))
    # Parse the payload JSON
    payload = parsed.select(from_json(col("data.payload"), payload_schema).alias("payload"))
    # Flatten
    flat = payload.select(
        col("payload.sensor_id").alias("sensor_id"),
        col("payload.modality").alias("modality")
    )

    # Compute running mean per sensor_id
    result = flat.groupBy("sensor_id").agg(avg("modality").alias("running_mean_modality"))

    query = (
        result.writeStream
        .outputMode("complete")
        .format("console")
        .option("truncate", False)
        .start()
    )

    query.awaitTermination()

    spark.stop()
