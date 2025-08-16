#imports
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, FloatType
from pyspark.sql.functions import from_json, col, avg, min, max, window,variance, to_timestamp
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import time

# creating cassandra keyspace,cluster, session, and table
def cassandra_schema():
    authentication = PlainTextAuthProvider(username = "cassandra", password = "cassandra")
    cluster = Cluster(['cassandra'], auth_provider = authentication)
    session = cluster.connect()
    session.execute(f"""
      CREATE KEYSPACE IF NOT EXISTS weather_ks
      WITH REPLICATION = {{'class':'SimpleStrategy','replication_factor':1}};
    """)
    session.set_keyspace("weather_ks")
    session.execute("""
        CREATE TABLE IF NOT EXISTS aggregates(
        metric text,
        window_start timestamp,
        window_end timestamp,
        avg_val float,
        min_val float,
        max_val float,
        var_val float,
        PRIMARY KEY ((metric), window_start, window_end)
        );
        """
    )
    session.shutdown()
    cluster.shutdown()

cassandra_schema()
time.sleep(2)


#Starts Spark / Configuring Spark to connect to Cassandra inside Docker.
spark = SparkSession.builder\
    .appName('KafkaToCassandra')\
    .config("spark.cassandra.connection.host", "cassandra")\
    .config("spark.cassandra.auth.username", "cassandra")\
    .config("spark.cassandra.auth.password", "cassandra")\
    .getOrCreate()


#Read from kafka (Connects to Kafka at kafka:9092, subscribes to topic weather-topic)
raw= spark.readStream\
    .format('kafka')\
    .option('kafka.bootstrap.servers', "kafka:9092")\
    .option("subscribe", "weather-topic")\
    .load()


# Defining schema for incoming Kafka messages.
schema = StructType() \
    .add('id', StringType()) \
    .add('time', StringType())\
    .add('metric',StringType())\
    .add("value",FloatType())

# pickup kafka message ->turns binary into string ->
# parse this string according to schema -> unpack the string into individual cols->
# creates a timestamp column. 
parsed = (raw.select(from_json(col("value").cast("string"), schema).alias('data'))\
    .select("data.*")\
    .withColumn("event_time",to_timestamp('time', "yyyy-MM-dd'T'HH:mm:ssX")))

#Aggregating data every 1 minute
aggregated = (parsed.withWatermark('event_time', '2 minutes')\
        .groupby(col("metric"), window(col("event_time"), "1 minute"))\
        .agg(avg("value").alias("avg_val"),
            min('value').alias('min_val'),
            max("value").alias("max_val"),
            variance("value").alias("var_val"))\
    .select(col("metric"),
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        "avg_val", "min_val", "max_val", "var_val"))


parsed.writeStream \
    .format('console')\
    .outputMode('append')\
    .option("truncate", False)\
    .start()\
    .awaitTermination()

# Proceed with Cassandra write
aggregated.writeStream \
    .format("org.apache.spark.sql.cassandra") \
    .option("checkpointLocation", "/tmp/checkpoints/aggregates") \
    .option("keyspace", "weather_ks") \
    .option("table", "aggregates") \
    .outputMode("append") \
    .start()\
    .awaitTermination()

# Await termination for query.
#spark.streams.awaitAnyTermination()



