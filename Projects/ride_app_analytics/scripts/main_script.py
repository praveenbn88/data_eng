from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, count, sum, avg, current_timestamp, window, round, current_date, \
    concat, lit, date_add
from pyspark.sql.types import StructType, StringType, TimestampType, DoubleType, IntegerType
from cassandra.cluster import Cluster
import uuid
import pyspark.sql.functions as F
import traceback
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)

logger = logging.getLogger(__name__)


################## Global Variables #########################

APP_NAME = "tenantstreamapp"
KAFKA_HOST = "broker:9092"
cassandra_keyspace = "spark_streaming"

checkpoint_dir = "/tmp/checkpoints_new_999"
################## Global Variables #########################


def create_spark_connection():
    """
    Create and return a SparkSession configured for Kafka and Cassandra integration.
    Returns:
        SparkSession: Configured Spark session object.
    """
    try:
        spark = SparkSession.builder \
                    .appName('SparkDataStreaming') \
                    .config('spark.jars.packages', "com.datastax.spark:spark-cassandra-connector_2.12:3.5.1,"
                                                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1") \
                    .config('spark.cassandra.connection.host', 'cassandra_db') \
                    .config('spark.sql.streaming.continuous.enabled', 'true') \
                    .getOrCreate()
        spark.sparkContext.setLogLevel("ERROR")

        spark.catalog.clearCache()
        return spark
    
    except Exception as e:
        logger.error(f"Error while creating spark connection: {e}")
        exit(1)


################################ Cassandra Setup ####################################################
def create_keyspace(session):
    """
    Create the Cassandra keyspace if it does not exist.
    Args:
        session: Cassandra session object.
    """
    session.execute(f"""
        CREATE KEYSPACE IF NOT EXISTS {cassandra_keyspace}
        WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1}
        """)
    
    logger.info("Keyspace created successfully")


def create_cassandra_connection():
    """
    Establish and return a connection to the Cassandra cluster.
    Returns:
        Cassandra session object or None if connection fails.
    """
    try:
        # Connection to Cassandra cluster
        cluster = Cluster(['cassandra_db'])
        cas_session = cluster.connect()
        logger.info("Cassandra connection created successfully")
        return cas_session
    
    except Exception as e:
        logger.error(f"Error while creating Cassandra connection: {e}")
        return None
    



def create_cassandra_table(session, table_name, schema, primary_key):
    """

    :param session: Cassandra driver session (e.g., from cassandra.cluster.Cluster.connect())  
    :param table_name: Table name as string  
    :param schema: List of tuples [(col_name, col_type), ...]  
                   e.g. [("first_name", "TEXT"), ("age", "INT"), ...]  
    :param primary_key: String or tuple/list of strings for the PK  
                        e.g. "username" or ("user_id", "event_time")  
    """

    col_defs = ",\n    ".join(f"{col} {dtype}" for col, dtype in schema)

    if isinstance(primary_key, (list, tuple)):
        pk = "(" + ", ".join(primary_key) + ")"
    else:
        pk = primary_key
    cql = f"""
    CREATE TABLE IF NOT EXISTS {cassandra_keyspace}.{table_name} (
        {col_defs},
        PRIMARY KEY ({pk})
    );
    """.strip()

    # Execute
    session.execute(cql)
    
    logger.info(f"Table {table_name} created successfully")



def write_to_cassandra(df, table_name):
    """
    Write a DataFrame to a Cassandra table, appending a last_modified_ts column.
    Args:
        df (DataFrame): Spark DataFrame to write.
        table_name (str): Target Cassandra table name.
    """
    try:
        selection_df = df.withColumn("last_modified_ts", F.current_timestamp())

        (selection_df.write.format("org.apache.spark.sql.cassandra")
                            .option('keyspace', f'{cassandra_keyspace}')
                            .mode("append") 
                            .option('table', f"{table_name}")
                            .save())
    except Exception as e:
        logger.error(f"Error while processing batch: {e}")
        logger.error(f"Error type: {type(e).__name__}")
        logger.error(f"Error details: {str(e)}")
        
        logger.error(f"Traceback: {traceback.format_exc()}")


################################ Cassandra Setup Ends ####################################################


uuid_udf = F.udf(lambda: str(uuid.uuid4()), StringType())



def return_write_stream(df, topic):
    """
    Write a DataFrame to a Kafka topic in append mode.
    Args:
        df (DataFrame): Spark DataFrame to write.
        topic (str): Kafka topic name (without app prefix).
    """
    logger.info(f"inside return_write_stream for topic {APP_NAME}_{topic}")
    '''return df.writeStream.outputMode(mode).format("kafka").queryName(name).option("kafka.bootstrap.servers",
                                                                                  KAFKA_HOST).option(
        "topic", f"{APP_NAME}_{topic}").option("checkpointLocation", f"/tmp/checkpoints/{topic}").start()'''

    df.write \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_HOST) \
        .option("topic", f"{APP_NAME}_{topic}") \
        .mode("append") \
        .save()



def write_to_multiple_sinks(topic, table_name=None):
    """
    Returns a function to process each micro-batch, writing to Cassandra and/or Kafka as needed.
    Args:
        topic (str): Kafka topic name.
        table_name (str, optional): Cassandra table name.
    Returns:
        function: Batch processing function for foreachBatch.
    """

    def process_batch(df, batch_id):
        print(f"Processing batch {batch_id}")
        try:
            df.cache()
            df.show(20,False)

            write_to_cassandra(df,table_name)
            #return_write_stream(df, topic )
        except Exception as e:
            logger.error(f"Error while processing batch: {e}")
            logger.error(f"Error type: {type(e).__name__}")
            logger.error(f"Error details: {str(e)}")
            
            logger.error(f"Traceback: {traceback.format_exc()}")

    return process_batch        





def read_from_kafka(spark):
    """
    Read streaming data from a Kafka topic and parse it into a structured DataFrame.
    Args:
        spark (SparkSession): Spark session object.
    Returns:
        DataFrame: Parsed DataFrame with taxi trip schema.
    """

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

    return info_df_fin



def sumFareWindowStream(info_df_fin):
    """
    Compute sum of fares, tips, and trip totals in 5-second windows with watermarking.
    Args:
        info_df_fin (DataFrame): Input DataFrame with trip data.
    Returns:
        StreamingQuery: The running streaming query object.
    """
    topic ="sumFareWindowStream"
    writemode="update"
    query_name = "sum_fare_window_stream"


    # fare distribution
    sum_fare_window = info_df_fin.withWatermark("event_timestamp", "10 seconds").groupBy(
        window("event_timestamp", "5 seconds")).agg(round(sum("fare"), 0).alias("total_fare"),
                                                    round(sum("tips"), 0).alias("tips_fare"),
                                                    round(sum("trip_total"), 0).alias(
                                                        "total_trip_total"))#.selectExpr("to_json(struct(*)) AS value")
    #sum_fare_window_stream = return_write_stream(sum_fare_window, "sumFareWindowStream", "update", "sum_fare_window_stream")
    sum_fare_window_stream =  sum_fare_window.writeStream \
                                .queryName(query_name) \
                                .foreachBatch(write_to_multiple_sinks(topic)) \
                                .option("checkpointLocation", f"{checkpoint_dir}/{topic}") \
                                .outputMode(writemode) \
                                .start()
    return sum_fare_window_stream


def tripsTotalStream(info_df_fin):
    """
    Compute total trips, fare, and averages for the current day in a streaming window.
    Args:
        info_df_fin (DataFrame): Input DataFrame with trip data.
    Returns:
        StreamingQuery: The running streaming query object.
    """
    # total so far
    topic ="tripsTotalStream"
    writemode="update"
    query_name = "trips_total_stream"
    today = current_date()
    specific_time = "00:00:00"  # Change this to your specific time
    specific_date_time = concat(today, lit(" "), lit(specific_time)).cast("timestamp")
    specific_time_data = info_df_fin.filter(col("event_timestamp") > specific_date_time) \
        .filter(col("event_timestamp") < date_add(today, 1))

    trips_total = specific_time_data.select(count("*").alias("trips_total"), round(sum("fare"), 0).alias("fare_total"),
                                            round(avg("tips"), 0).alias("tips_avg"),
                                            round(avg("trip_total"), 0).alias("trip_total_avg"))#.selectExpr( "to_json(struct(*)) AS value")
    ##trips_total_stream = return_write_stream(trips_total, "tripsTotalStream", "complete", "trips_total_stream")

    trips_total_stream =  trips_total.writeStream \
                            .queryName(query_name) \
                            .foreachBatch(write_to_multiple_sinks(topic)) \
                            .option("checkpointLocation", f"{checkpoint_dir}/{topic}") \
                            .outputMode(writemode) \
                            .start()
    return trips_total_stream


def hotspotCommunityPickupWindowStream(info_df_fin):
    """
    Detect demand hotspots by community area using windowed counts, and write to Cassandra.
    Args:
        info_df_fin (DataFrame): Input DataFrame with trip data.
    Returns:
        StreamingQuery: The running streaming query object.
    """
    topic ="hotspotCommunityPickupWindowStream"
    writemode="append"
    query_name = "hot_spot_community_pickup_window_stream"
    table_name = query_name

    cassandra_session = create_cassandra_connection()

    cassandra_schema = [
    ("id","uuid"),
    ("pickup_community_area", "TEXT"),
    ("count", "int"),
    ("window_start", "TIMESTAMP"),
    ("window_end", "TIMESTAMP"),
    ("last_modified_ts", "TIMESTAMP")
        ]

    # 3. Call helper
    create_cassandra_table(
        cassandra_session,
        table_name=table_name,
        schema=cassandra_schema,
        primary_key="id"
    )
    # hot spot pickup community area
    hot_spot_community_pickup_window = info_df_fin.withWatermark("event_timestamp", "10 seconds").groupBy(
        window("event_timestamp", "1 minute", "30 seconds"), "pickup_community_area").count().filter(
        col("pickup_community_area").isNotNull())##.selectExpr("to_json(struct(*)) AS value")
    
    hot_spot_community_pickup_window = hot_spot_community_pickup_window.withColumn("id", uuid_udf()).withColumn("last_modified_ts", F.current_timestamp()).select("id","pickup_community_area","count",F.col("window.start").alias("window_start"), F.col("window.end").alias("window_end"),"last_modified_ts")
    

    hot_spot_community_pickup_window_stream =  hot_spot_community_pickup_window.writeStream \
                        .queryName(query_name) \
                        .foreachBatch(write_to_multiple_sinks(topic,table_name)) \
                        .option("checkpointLocation", f"{checkpoint_dir}/{topic}") \
                        .trigger(processingTime="30 seconds") \
                        .outputMode(writemode) \
                        .start()
    '''hot_spot_community_pickup_window_stream = return_write_stream(hot_spot_community_pickup_window,
                                                                "hotspotCommunityPickupWindowStream", "append",
                                                                "hot_spot_community_pickup_window_stream")'''
    return hot_spot_community_pickup_window_stream


def hotspotWindowStream(info_df_fin):
    """
    Detect demand hotspots by pickup location (lat/lon), compute dynamic pricing, and write results.
    Args:
        info_df_fin (DataFrame): Input DataFrame with trip data.
    Returns:
        StreamingQuery: The running streaming query object.
    """
    topic ="hotspotWindowStream"
    writemode="append"
    query_name = "hot_spot_window_stream"
    # hot spot pickup location
    info_df_fin = info_df_fin.withColumn("geom", F.concat(F.round(F.col("pickup_centroid_latitude").cast("double"),3).cast("string"), F.lit(','), F.round(F.col("pickup_centroid_longitude").cast("double"),3).cast("string")    ))
    info_df_fin=info_df_fin.select("geom","event_timestamp","trip_total")
    hot_spot_window = info_df_fin.withWatermark("event_timestamp", "10 seconds") \
    .groupBy(
        window("event_timestamp", "1 minute", "30 seconds"), "geom").agg(count("*").alias("demand"))#.selectExpr("to_json(struct(*)) AS value")
    

    hot_spot_window = hot_spot_window.select("geom","demand",F.col("window.start").alias("win_start"), F.col("window.end").alias("win_end"))   
    
    df_with_region = info_df_fin.alias('a').join(
            hot_spot_window.alias('b'),
            [
                info_df_fin["geom"] == hot_spot_window["geom"], F.expr("a.event_timestamp >= b.win_start and a.event_timestamp<b.win_end")
            ]
        ).drop(hot_spot_window["geom"])

    df_with_surcharge = df_with_region.withColumn(
    "surcharge_multiplier",
            F.when(F.col("demand") > 2, 1.2).otherwise(1.0)
        ).withColumn(
            "dynamic_price",
            col("trip_total") * col("surcharge_multiplier")
        ).selectExpr("to_json(struct(*)) AS value")
    #hot_spot_window_stream = return_write_stream(hot_spot_window, "hotspotWindowStream", "append", "hot_spot_window_stream")\
    
    hot_spot_window_stream =  df_with_surcharge.writeStream \
                        .queryName(query_name) \
                        .foreachBatch(write_to_multiple_sinks(topic)) \
                        .option("checkpointLocation", f"{checkpoint_dir}/{topic}") \
                        .trigger(processingTime="1 minute") \
                        .outputMode(writemode) \
                        .start()
    return hot_spot_window_stream





if __name__=="__main__":

     # Create Spark connection
    spark = create_spark_connection()

    if spark is not None:
        # Create connection to Kafka with Spark
        info_df_fin = read_from_kafka(spark)
        hot_spot_window_stream = hotspotWindowStream(info_df_fin)
        sum_fare_window_stream = sumFareWindowStream(info_df_fin)
        
        hot_spot_community_pickup_window_stream = hotspotCommunityPickupWindowStream(info_df_fin)
        trips_total_stream = tripsTotalStream(info_df_fin)

        sum_fare_window_stream.awaitTermination()
        hot_spot_window_stream.awaitTermination()
        hot_spot_community_pickup_window_stream.awaitTermination()
        trips_total_stream.awaitTermination()
