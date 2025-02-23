from datetime import datetime
import uuid
import pandas as pd
import mysql.connector
import os 


script_name = os.path.basename(__file__)

########################### Configure these variables if required ############################################

table_name = 'spark_framework_stats'
final_stats_columns = ['year','month','day','process_name','sql_id','sql_status','src_paths','src_file_formats','src_no_of_files','src_size_in_gb','src_counts','dest_path','dest_no_of_files','dest_size_in_gb',
                       'dest_count','dest_schema','dest_file_format','dest_num_partitions','dest_partition_cols','dest_repartition_no','duration_in_mins','timestamp','response']


##############################################################################################################

## Global Variables - DO not touch these
conn = None
cursor = None

def custom_log(msg):
    print(f"{datetime.now()} {script_name} {msg}")


def create_db(cursor,db_name):
    cursor.execute(f"CREATE DATABASE IF NOT EXISTS {db_name}")




def database_conn_details():
    global conn 
    global cursor
    try:
        db_name = "spark_framework"
        password = "password"
        # Establish connection
        conn = mysql.connector.connect(
            host="localhost",
            user="root", 
            password=password
        )
        # Create a cursor object to execute SQL commands
        cursor = conn.cursor()
        create_db(cursor,db_name )
        cursor.execute(f"USE {db_name};")
        custom_log(f"-> Database Connected succeeded")
        return cursor
    except Exception as e:
        custom_log(f"Caught Exception inside database_conn_details() function. Unable to create DB connection. Exception -> {e}")
        exit(1)


#Success and api Errors table
def create_spark_framework_stats_table(table_name):
    try:
        cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            id INT AUTO_INCREMENT PRIMARY KEY,
            year INT,
            month INT, 
            day INT,
            process_name VARCHAR(50),
            sql_id VARCHAR(50),
            sql_status VARCHAR(15),
            src_paths VARCHAR(100),
            src_file_formats VARCHAR(50),
            src_no_of_files INT UNSIGNED,
            src_size_in_gb VARCHAR(15),
            src_counts BIGINT UNSIGNED,
            dest_path VARCHAR(100),
            dest_no_of_files INT UNSIGNED,
            dest_size_in_gb VARCHAR(15),
            dest_count BIGINT UNSIGNED,
            dest_schema VARCHAR(200),
            dest_file_format VARCHAR(15),
            dest_num_partitions INT UNSIGNED,  
            dest_partition_cols VARCHAR(15),
            dest_repartition_no INT UNSIGNED,
            duration_in_mins SMALLINT UNSIGNED,
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            response VARCHAR(200)
        )
    """)
        custom_log(f"Successfully created table {table_name}")
    
    except Exception as e:
        custom_log(f"ERROR!!! Caught Exception inside create_fact_table() function. Unable to create table {table_name} Exception -> {e}")
        exit(1)    




def insert_data(values):
    try:
        query = f"INSERT INTO {table_name} (year, 'month','day','process_name','sql_id','sql_status','src_paths','src_file_formats','src_no_of_files','src_size_in_gb','src_counts','dest_path','dest_no_of_files','dest_size_in_gb',
                       'dest_count','dest_schema','dest_file_format','dest_num_partitions','dest_partition_cols','dest_repartition_no','duration_in_mins','response' ) VALUES ( %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
        cursor.executemany(query, values)  # Insert multiple rows
        conn.commit()  # Commit changes

    except Exception as e:
        custom_log(f"ERROR!!! Caught Exception inside insert_data() function. Unable to insert data into table {table_name} Exception -> {e}")
        exit(1)        



def query_data(query,cursor):
    # Fetch all data from the table
    try:
        #custom_log(f"Executing select query -> {query}")
        cursor.execute(f"{query}")
        rows = cursor.fetchall()
        columns = [col[0] for col in cursor.description]
        records_list = [list(row) for row in rows]
        #custom_log(f"records_list is {records_list}") 
        return records_list, columns
        '''df = pd.DataFrame(rows, columns=columns)
        print(df)'''
    except mysql.connector.Error as err:
        custom_log(f"ERROR!!! mysql.connector.Error: {err}")
        exit(1)    
    except Exception as e:   
        custom_log(f"ERROR!!! -> {e}")     
        exit(1)




def initiate_db(delivery_type,run_id_tmp):
    global fact_tables
    global agg_tables
    global tables_dict
    global agg_tables_dict
    global run_id
    try:
        return_val_cursor = database_conn_details()
        create_spark_framework_stats_table(table_name)

        return return_val_cursor           
    except Exception as e:
        custom_log(f"ERROR!!! Caught Exception inside initiate_db(). {e}")
        exit(1)

