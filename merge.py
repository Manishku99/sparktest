import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Define your Kafka and Iceberg parameters
KAFKA_BOOTSTRAP_SERVERS = "broker:29092"
KAFKA_TOPIC = "pricedebt3"
ICEBERG_TABLE = "glue.icedbstreaming.icetblstream8"

# Define the schema of your Kafka value (adjust as needed)
kafka_schema = StructType([
    StructField("globalId", StringType(), False),
    StructField("modalClass", StringType(), True),
    StructField("askPrice", IntegerType(), True),
    StructField("bidPrice", IntegerType(), True)
])

# Initialize a SparkSession (the one for iceberg; do this second)
conf = (
    pyspark.SparkConf()
        .setAppName('app_name')
    #packages
        .set('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3,org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2,software.amazon.awssdk:bundle:2.20.131,software.amazon.awssdk:url-connection-client:2.20.131,org.apache.spark:spark-avro_2.12:3.5.3')
    #SQL Extensions
        .set('spark.sql.extensions', 'org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions')
    #Configuring Catalog
        .set('spark.sql.catalog.glue', 'org.apache.iceberg.spark.SparkCatalog')
        .set('spark.sql.catalog.glue.catalog-impl', 'org.apache.iceberg.aws.glue.GlueCatalog')
        .set('spark.sql.catalog.glue.warehouse', 's3://warehouseice/icebergdata/')
        .set('spark.sql.catalog.glue.io-impl', 'org.apache.iceberg.aws.s3.S3FileIO')    
)

spark = SparkSession.builder.config(conf=conf).getOrCreate()
print("Spark Running")

# Read streaming data from Kafka
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
    .option("subscribe", KAFKA_TOPIC) \
    .option("startingOffsets", "earliest") \
    .load()


# Parse the Kafka value as JSON
parsed_df = kafka_df.select(
    from_json(col("value").cast("string"), kafka_schema).alias("data")
).select("data.*")

# Define the merge (upsert) logic using Iceberg's MERGE INTO
def foreach_batch_function(batch_df, batch_id):
    if not batch_df.isEmpty():
        batch_df.createOrReplaceTempView("updates_in_batch")
        batch_df.sparkSession.sql("SELECT * FROM updates_in_batch").show()
        
        merge_sql = f"""
            MERGE INTO glue.icedbstreaming.icetblstream8 t
            USING updates_in_batch s
            ON t.globalId = s.globalId
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
        """
        batch_df.sparkSession.sql(merge_sql)

# Write stream with foreachBatch for upsert
query = parsed_df.writeStream \
    .foreachBatch(foreach_batch_function) \
    .outputMode("update") \
    .option("checkpointLocation", "/tmp/spark_kafka_iceberg_checkpoint") \
    .start()

query.awaitTermination()
