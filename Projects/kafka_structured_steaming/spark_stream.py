import logging
from datetime import datetime
import traceback
from cassandra.cluster import Cluster
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
import pyspark.sql.functions as F

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)

logger = logging.getLogger(__name__)

def create_keyspace(session):
    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS spark_streaming
        WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1}
        """)
    
    logger.info("Keyspace created successfully")

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

'''
def insert_data(session, **kwargs):
    logger.info("Inserting data")

    first_name = kwargs.get('first_name')
    last_name = kwargs.get('last_name')
    gender = kwargs.get('gender')
    address = kwargs.get('address')
    postcode = kwargs.get('post_code')
    email = kwargs.get('email')
    username = kwargs.get('username')
    logger.info(f"Username: {username}")
    dob = kwargs.get('dob')
    registered_date = kwargs.get('registered_date')
    phone = kwargs.get('phone')
    picture = kwargs.get('picture')

    try:
        session.execute("""
            INSERT INTO spark_streaming.created_users (first_name, last_name, gender, address,
                        post_code, email, username, dob, registered_date, phone, picture)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (first_name, last_name, gender, address,
              postcode, email, username, dob, registered_date, phone, picture))
        logger.info(f"Data inserted for {first_name} {last_name}")

    except Exception as e:
        logger.error(f"Error while inserting data: {e}")

'''

def create_spark_connection():
    s_conn = None

    try:
        s_conn = SparkSession.builder \
            .appName('SparkDataStreaming') \
            .config('spark.jars.packages', "com.datastax.spark:spark-cassandra-connector_2.12:3.5.1,"
                                           "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1") \
            .config('spark.cassandra.connection.host', 'cassandra_db') \
            .config('spark.sql.streaming.continuous.enabled', 'true') \
            .getOrCreate()
        s_conn.sparkContext.setLogLevel("ERROR")
        logger.info("Spark connection created successfully")

    except Exception as e:
        logger.error(f"Error while creating spark connection: {e}")
    
    return s_conn

def connect_to_kafka(spark_conn):
    spark_df = None
    try:
        spark_df = spark_conn.readStream \
            .format('kafka') \
            .option('kafka.bootstrap.servers', 'broker:9092') \
            .option('subscribe', 'users_created_new') \
            .option('startingOffsets', 'earliest') \
            .load()
        logger.info("Kafka dataframe created successfully")
    except Exception as e:
        logger.error(f"Kafka dataframe could not be created because: {e}")
        logger.error(f"Error type: {type(e).__name__}")
        logger.error(f"Error details: {str(e)}")
        logger.error(f"Traceback: {traceback.format_exc()}")
    
    if spark_df is None:
        logger.error("Failed to create Kafka dataframe")
    
    return spark_df

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

def create_selection_df_from_kafka(spark_df):
    '''schema = StructType([
        StructField("first_name", StringType(), False),
        StructField("last_name", StringType(), False),
        StructField("gender", StringType(), False),
        StructField("address", StringType(), False),
        StructField("post_code", StringType(), False),
        StructField("email", StringType(), False),
        StructField("username", StringType(), False),
        StructField("dob", StringType(), False),
        StructField("registered_date", StringType(), False),
        StructField("phone", StringType(), False),
        StructField("picture", StringType(), False),
        StructField("last_modified_ts", TimestampType(), False)
    ])'''
    schema = StructType([
        StructField("username", StringType(), False),
        StructField("last_modified_ts", TimestampType(), False)
    ])
    

    sel = spark_df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col("value"), schema).alias("data")) \
        .select("data.*")
    logger.info("Selection dataframe created successfully")
    return sel



def process_batch(selection_df, batch_id):
    print(f"Processing batch {batch_id}")
    try:
        selection_df = selection_df.withColumn("last_modified_ts", F.current_timestamp())
        selection_df.show()

        (selection_df.write.format("org.apache.spark.sql.cassandra")
                            .option('keyspace', 'spark_streaming')
                            .mode("append") 
                            .option('table', 'created_users')
                            .save())
    except Exception as e:
        logger.error(f"Error while processing batch: {e}")
        logger.error(f"Error type: {type(e).__name__}")
        logger.error(f"Error details: {str(e)}")
        
        logger.error(f"Traceback: {traceback.format_exc()}")


if __name__ == "__main__":
    # Create Spark connection
    spark_conn = create_spark_connection()

    if spark_conn is not None:
        # Create connection to Kafka with Spark
        spark_df = connect_to_kafka(spark_conn)
        selection_df = create_selection_df_from_kafka(spark_df)

        logger.info("Selection dataframe schema:")
        selection_df.printSchema()

        # Create Cassandra connection
        session = create_cassandra_connection()
        
        if session is not None:
            create_keyspace(session)
            create_table(session)

            '''streaming_query = (selection_df.writeStream
                    .foreachBatch(process_batch)
                    .option('checkpointLocation', '/tmp/checkpoint_new/')
                    .trigger(continuous='1 second')
                    .start())'''
            #selection_df = selection_df.withColumn("last_modified_ts", F.expr("current_timestamp() - interval 1 minute"))
            selection_df = selection_df.withWatermark("last_modified_ts", "10 seconds")

            selection_df = selection_df.groupBy(F.window(F.col("last_modified_ts"), "1 minute")).count()
            
            streaming_query=(selection_df.writeStream
                    .format("console") 
                    .outputMode("update")
                    .option("truncate", "false")
                    .option('checkpointLocation', '/tmp/checkpoint_new1911_new_earliest_1/')
                    .trigger(processingTime='5 seconds')
                    .start())
            
            streaming_query.awaitTermination()
            
            
