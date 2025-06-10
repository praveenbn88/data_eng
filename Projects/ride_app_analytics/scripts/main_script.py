from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, count, sum, avg, current_timestamp, window, round, current_date, \
    concat, lit, date_add
from pyspark.sql.types import StructType, StringType, TimestampType, DoubleType, IntegerType
from cassandra.cluster import Cluster
import logging


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)

logger = logging.getLogger(__name__)


APP_NAME = "tenantstreamapp"
KAFKA_HOST = "broker:9092"

spark = SparkSession.builder \
            .appName('SparkDataStreaming') \
            .config('spark.jars.packages', "com.datastax.spark:spark-cassandra-connector_2.12:3.5.1,"
                                           "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1") \
            .config('spark.cassandra.connection.host', 'cassandra_db') \
            .config('spark.sql.streaming.continuous.enabled', 'true') \
            .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

spark.catalog.clearCache()


################################ Cassandra Setup ####################################################
def create_keyspace(session):
    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS spark_streaming
        WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1}
        """)
    
    logger.info("Keyspace created successfully")


def create_cassandra_connection():
    try:
        # Connection to Cassandra cluster
        cluster = Cluster(['cassandra_db'])
        cas_session = cluster.connect()
        logger.info("Cassandra connection created successfully")
        return cas_session
    
    except Exception as e:
        logger.error(f"Error while creating Cassandra connection: {e}")
        return None
    



def create_table(session):
    session.execute("""
        CREATE TABLE IF NOT EXISTS spark_streaming.created_users (
            first_name TEXT,
            last_name TEXT,
            gender TEXT,
            address TEXT,
            post_code TEXT,
            email TEXT,
            username TEXT PRIMARY KEY,
            dob TEXT,
            registered_date TEXT,
            phone TEXT,
            picture TEXT,
            last_modified_ts timestamp);
        """)
    
    logger.info("Table created successfully")


################################ Cassandra Setup Ends ####################################################








def return_write_stream(df, topic, mode, name):
    return df.writeStream.outputMode(mode).format("kafka").queryName(name).option("kafka.bootstrap.servers",
                                                                                  KAFKA_HOST).option(
        "topic", f"{APP_NAME}_{topic}").option("checkpointLocation", f"/tmp/checkpoints/{topic}").start()



def read_from_kafka():

    schema = StructType().add("trip_id", StringType(), True) \
        .add("taxi_id", StringType(), True) \
        .add("trip_start_timestamp", TimestampType(), True) \
        .add("trip_end_timestamp", TimestampType(), True) \
        .add("trip_seconds", IntegerType(), True) \
        .add("trip_miles", DoubleType(), True) \
        .add("pickup_census_tract", DoubleType(), True) \
        .add("dropoff_census_tract", DoubleType(), True) \
        .add("pickup_community_area", IntegerType(), True) \
        .add("dropoff_community_area", DoubleType(), True) \
        .add("fare", DoubleType(), True) \
        .add("tips", DoubleType(), True) \
        .add("tolls", DoubleType(), True) \
        .add("extras", DoubleType(), True) \
        .add("trip_total", DoubleType(), True) \
        .add("payment_type", StringType(), True) \
        .add("company", StringType(), True) \
        .add("pickup_centroid_latitude", DoubleType(), True) \
        .add("pickup_centroid_longitude", DoubleType(), True) \
        .add("pickup_centroid_location", StringType(), True) \
        .add("dropoff_centroid_latitude", DoubleType(), True) \
        .add("dropoff_centroid_longitude", DoubleType(), True) \
        .add("dropoff_centroid_location", StringType(), True) \
        .add("event_timestamp", TimestampType(), True)

    # Create DataFrame representing the stream of input lines from connection to localhost:9999
    df = (
        spark.readStream.format("kafka")
        .option('kafka.bootstrap.servers', KAFKA_HOST)
        .option("subscribe", f"taxi_trips")
        .option("failOnDataLoss", "false")
        .option("startingOffsets", "latest")
        .load()
    )

    base_df = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)", "timestamp")

    info_df = base_df.select(from_json(col("value"), schema).alias("trip_schema"), "timestamp")
    info_df_fin = info_df.select("trip_schema.*", "timestamp")

    info_df_fin.cache()
    return info_df_fin



def sumFareWindowStream(info_df_fin):
    # fare distribution
    sum_fare_window = info_df_fin.withWatermark("event_timestamp", "10 seconds").groupBy(
        window("event_timestamp", "5 seconds")).agg(round(sum("fare"), 0).alias("total_fare"),
                                                    round(sum("tips"), 0).alias("tips_fare"),
                                                    round(sum("trip_total"), 0).alias(
                                                        "total_trip_total")).selectExpr(
        "to_json(struct(*)) AS value")
    sum_fare_window_stream = return_write_stream(sum_fare_window, "sumFareWindowStream", "update", "sum_fare_window_stream")
    return sum_fare_window_stream


def tripsTotalStream(info_df_fin):
    # total so far
    today = current_date()
    specific_time = "00:00:00"  # Change this to your specific time
    specific_date_time = concat(today, lit(" "), lit(specific_time)).cast("timestamp")
    specific_time_data = info_df_fin.filter(col("event_timestamp") > specific_date_time) \
        .filter(col("event_timestamp") < date_add(today, 1))

    trips_total = specific_time_data.select(count("*").alias("trips_total"), round(sum("fare"), 0).alias("fare_total"),
                                            round(avg("tips"), 0).alias("tips_avg"),
                                            round(avg("trip_total"), 0).alias("trip_total_avg")).selectExpr(
        "to_json(struct(*)) AS value")
    trips_total_stream = return_write_stream(trips_total, "tripsTotalStream", "complete", "trips_total_stream")
    return trips_total_stream


def hotspotCommunityPickupWindowStream(info_df_fin):
    # hot spot pickup community area
    hot_spot_community_pickup_window = info_df_fin.withWatermark("event_timestamp", "10 seconds").groupBy(
        window("event_timestamp", "1 minute", "30 seconds"), "pickup_community_area").count().filter(
        col("pickup_community_area").isNotNull()).selectExpr(
        "to_json(struct(*)) AS value")
    hot_spot_community_pickup_window_stream = return_write_stream(hot_spot_community_pickup_window,
                                                                "hotspotCommunityPickupWindowStream", "append",
                                                                "hot_spot_community_pickup_window_stream")
    return hot_spot_community_pickup_window_stream


def hotspotWindowStream(info_df_fin):
    # hot spot pickup location
    hot_spot_window = info_df_fin.withWatermark("event_timestamp", "10 seconds").groupBy(
        window("event_timestamp", "1 minute", "30 seconds"), "pickup_centroid_location").count().selectExpr(
        "to_json(struct(*)) AS value")
    hot_spot_window_stream = return_write_stream(hot_spot_window, "hotspotWindowStream", "append", "hot_spot_window_stream")
    return hot_spot_window_stream


if __name__=="__main__":

    sum_fare_window_stream.awaitTermination()
    hot_spot_window_stream.awaitTermination()
    hot_spot_community_pickup_window_stream.awaitTermination()
    trips_total_stream.awaitTermination()
