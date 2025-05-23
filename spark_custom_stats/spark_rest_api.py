###################################

import importlib.util
import subprocess
import pandas as pd

# Check if pandas is installed
if importlib.util.find_spec("pandas") is None:
    print("pandas is not installed. Installing...")
    result = subprocess.run(["pip3", "install", "pandas"])
    if result.returncode!=0:
        print(f"F_ERROR!!!! Couldn't install pandas package with pip3 install pandas. Please check. Exiting")
        exit(1)

    print(f"pandas module successfully installed")



# Check if boto3 is installed
if importlib.util.find_spec("requests") is None:
    print("requests is not installed. Installing...")
    result = subprocess.run(["pip3", "install", "requests"])
    if result.returncode!=0:
        print(f"F_ERROR!!!! Couldn't install requests package with pip3 install requests. Please check. Exiting")
        exit(1)

    print(f"requests module successfully installed")



##################################


import requests
import json
import re
from datetime import datetime
import time
#import db_script as db_script

# Sample Example of how a API request URL would look like
#url = "http://xx.x.x.xxxz:18080/api/v1/applications/application_1710224122225_0012/sql?offset=0&length=1"



dt = datetime.now()
year = dt.year
month = dt.month #'%02d' % dt.month
day = dt.day #'%02d' % dt.day
ts = datetime.now().strftime("%Y_%m_%d_%H_%M_%S_%f")

## &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&& ##
## &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&& ##
## &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&& ##
## &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&& ##
## &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&& ##
################################################ UPDATE THESE VARIABLES AS PER YOUR USE CASE ###############################################


ip_final = "localhost"
spark_ui_port = 4040 ##18080

# These are the final list of columns that appear in the stats
final_stats_columns = ['year','month','day','process_name','sql_id','sql_status','src_paths','src_file_formats','src_no_of_files','src_size_in_gb','src_counts','dest_path','dest_no_of_files','dest_size_in_gb',
                       'dest_count','dest_schema','dest_file_format','dest_num_partitions','dest_partition_cols','dest_repartition_no','duration_in_mins','timestamp']


threshold_out_final_no_files = 5000


######################################################################################################################
## &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&& ##
## &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&& ##
## &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&& ##
## &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&& ##
## &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&& ##


def custom_log(msg):

    ''' You can implement your own version of logging here. I'm just printing the message in this case
    '''
    ts = datetime.now()
    print(f"{ts} -> {msg}")






def generate_api_url(spark):
    #custom_log(f"Inside generate_api_url function with argument spark -> {spark}")
    application_id = spark.sparkContext.applicationId
    custom_log(f"application_id is {application_id}")
    ''' #Uncomment the below lines if you are running in a cluster environment on some cloud platform
    ip_raw = spark.sparkContext.uiWebUrl  
    ip_intermediate = ip_raw.split(':')[1].replace("//","")  ##['http', '//ip-xx-x-x-xxx.ec2.internal', '4041']
    ip_final = ip_intermediate.split(".")[0].replace("ip-","").replace("-",".")  # 'xx.x.x.xxx'''
    custom_log(f"ip of master node is {ip_final}")
    url = f"http://{ip_final}:{spark_ui_port}/api/v1/applications/{application_id}/sql?offset=0&length=10000"
    custom_log(f"API url is {url}")
    return url



def check_if_this_is_required_path(actual_path, save_path):
    path1 = actual_path[:-1] if actual_path[-1]=='/' else actual_path
    path2 = save_path[:-1] if save_path[-1]=='/' else save_path
    if path1.lower().strip()==path2.lower().strip():
        return True

    return False


def write_to_db(output_df):
    '''Configure your DB to write this data. Below is a sample script for mysql'''
    return
    try:
        cursor = db_script.initiate_db()
        values = [tuple(x) for x in output_df.values]
        db_script.insert(values)

    except Exception as e:
        custom_log(f"Caught an exception at write_to_db function. Exception -> {e}")   
        exit(1) 




def kb_to_gb(kb):
    return kb / (1024.0 * 1024)


def mb_to_gb(mb):
    return mb / 1024.0


def tb_to_gb(tb):
    return tb * (1024.0 ** 2)

def bytes_to_gb(bytes):
    return bytes / (1024.0 ** 3)


def spark_rest_api_call(url):
    # Make a GET request to the API endpoint
    import requests
    response = requests.get(url)
    if response.status_code == 200:
        return response
    else:
        custom_log(f"Error while making api call. ")
        custom_log(f"Here is the response: {response}")
        exit(1)
    

def get_parameters_of_sql_id(final_response,sql_id):


    custom_log("\n")
    custom_log(f"#"*60)
    custom_log("\n")
    custom_log(f"running for sql_id {sql_id}")
    sql_id_info = final_response[sql_id]
    success_job_ids_len = len(sql_id_info['successJobIds'])
    custom_log(f"there were {success_job_ids_len} successful job(s)")
    sql_status = sql_id_info['status'] if success_job_ids_len>0 else "FAILED"# status can be COMPLETED, FAILED, RUNNING
    actual_sql_id = sql_id_info['id']
    if sql_status=="RUNNING":
        custom_log(f"It is still in running status. Skipping this sql_id={sql_id}")
        return

    ##node
    nodes_info_list = sql_id_info['nodes']
    files_read = {}
    files_read_formats = {}
    file_sizes = {}
    input_rows = {}
    output_rows = {}
    output_size = {}
    output_number_of_files = {}
    output_number_of_dynamic_partitions = {}
    output_schema = {}
    output_path = {}
    output_partition_cols = {}
    output_repartition_numbers = {}
    output_file_format = {}
    output_duration = {}


    ## Getting Total Duration of SQL
    keys_required = ['status','description','submissionTime','duration']
    sql_key_info = {}
    for key in keys_required:
        val = sql_id_info[key]
        sql_key_info[key] = val

    output_duration[0] = int(sql_key_info['duration'])/(60 * 1000)


    if "showString " in sql_id_info['description'] or "collect " in sql_id_info['description'] or "toPandas " in sql_id_info['description'] or "load " in sql_id_info['description']:
        custom_log(f"Description is {sql_id_info['description']}. Continuing to next sql")
        return

    ## Handle Count Jobs
    if "count " in sql_id_info['description']:
        custom_log(f"Description is {sql_id_info['description']}. Continuing to next sql")
        return

    if "save " in sql_id_info['description'] or "csv " in sql_id_info['description'] or "parquet " in sql_id_info['description']:
        custom_log("caught inside parquet")
        plan_description = sql_id_info['planDescription']
        if "InsertIntoHadoopFsRelationCommand" not in plan_description:
            custom_log(f"Description is {sql_id_info['description']}")
            custom_log(f"This is a load statement. Continuing with next sql id")
            return
        output_schema_temp = plan_description.split(' ')[-1]
        output_schema[0]=output_schema_temp
        #description_list = sql_id_info['planDescription'].split(',')

        find_bracket_index = 0
        ## if partitioning exists
        find_str = "__partition_columns="
        partition_cols_start_index = plan_description.find(f"{find_str}")
        output_info_start_index = partition_cols_start_index
        if partition_cols_start_index!=-1:
            partition_cols_end_index = plan_description.find("]", partition_cols_start_index)
            output_partition_cols[0]=plan_description[partition_cols_start_index + len(find_str):partition_cols_end_index+1]
            find_bracket_index = partition_cols_end_index + 1

            output_info_end_index = plan_description.find("]", find_bracket_index)
            output_info = plan_description[partition_cols_start_index:output_info_end_index].split(',')
            #custom_log(output_info)
            output_dict = {}
            for part in output_info:
                key_end_index = part.find('=')
                output_dict[part[:key_end_index].strip()] = part[key_end_index + 1:].strip()

            custom_log(f"output_dict is {output_dict}")
            output_path[0] = output_dict['path']

        else:
            output_info_start_index = plan_description.find("[path")
            output_info_end_index = plan_description.find("/]", output_info_start_index)
            output_info = plan_description[output_info_start_index+1:output_info_end_index].split(',')
            custom_log(output_info)
            output_dict = {}
            for part in output_info:
                key_end_index = part.find('=')
                output_dict[part[:key_end_index].strip()]=part[key_end_index+1:].strip()
            custom_log(f"output_dict is {output_dict}")
            output_path[0] = output_dict['path']
            #custom_log(output_dict)

        custom_log(f"OUTSIDE IF")

        #get write format  ['InsertIntoHadoopFsRelationCommand s3://180bytwo-tmp/temp/test_csv', ' false', ' CSV', '']
        write_file_format = plan_description[:output_info_start_index-1].split(',')[-2]
        output_file_format[0] = write_file_format


        ### If Repartitioning exists
        find_str = "+- Repartition "
        repartition_start_index = plan_description.find(f"{find_str}")
        if repartition_start_index!=-1:
            repartition_end_index = plan_description.find(",",repartition_start_index + len(find_str))
            output_repartition_numbers[0] = int(plan_description[repartition_start_index + len(find_str):repartition_end_index].strip())
     
    ## Get number of output rows, sizes of files read etc info
    for node_dict in nodes_info_list:
        nodeID = node_dict['nodeId']
        metrics_list = node_dict['metrics']
        if 'Scan ' in node_dict['nodeName']:
            if nodeID in files_read_formats:
                files_read_formats[nodeID] = files_read_formats[nodeID].append(node_dict['nodeName'].split(' ')[1].strip())
            else:
                files_read_formats[nodeID]=[node_dict['nodeName'].split(' ')[1].strip()]
            for metric_dict in metrics_list:
                if metric_dict['name']=='number of files read':
                    files_read[nodeID] = int(re.sub(',', '', metric_dict['value']))

                elif metric_dict['name']=='size of files read':
                    file_sizes[nodeID]=metric_dict['value']

                elif metric_dict['name']=='number of output rows':
                    input_rows[nodeID] = int(re.sub(',', '', metric_dict['value']))

        if node_dict['nodeName']=='Execute InsertIntoHadoopFsRelationCommand':
            metrics_list = node_dict['metrics']
            for metric_dict in metrics_list:
                if metric_dict['name']=='number of output rows':
                    output_rows[nodeID] = int(re.sub(',', '', metric_dict['value']))
                elif metric_dict['name']=='written output':
                    output_size[nodeID] = metric_dict['value']
                elif metric_dict['name']=='number of written files':
                    output_number_of_files[nodeID] = int(re.sub(',', '', metric_dict['value']))
                elif metric_dict['name'] == 'number of dynamic part':
                    temp_var = int(re.sub(',', '', metric_dict['value']))
                    if temp_var!=0:
                        output_number_of_dynamic_partitions[nodeID] = temp_var




    custom_log("\n\n")

    ### Summarising Everything from above
    out_final_duration = round(output_duration[0],2)

    inp_final_no_files =  files_read #[val for key,val in files_read.items()]
    inp_final_format_of_files = files_read_formats #list(set(val for key,val in files_read_formats.items()))

    inp_final_record_count = input_rows #[val for key,val in input_rows.items()]


    out_final_no_files = sum([val for key, val in output_number_of_files.items()])
    out_final_format_of_file = list(set(val for key, val in output_file_format.items()))
    out_final_format_of_file = '' if not (out_final_format_of_file) else out_final_format_of_file[0]
    out_final_schema = list(set(val for key, val in output_schema.items()))
    out_final_schema = '' if not(out_final_schema) else out_final_schema[0]

    out_final_record_count = sum([val for key, val in output_rows.items()])
    out_final_path = list(set(val for key, val in output_path.items()))
    out_final_path = '' if not (out_final_path) else out_final_path[0]
    out_final_partition_cols = output_partition_cols #list(set(val for key, val in output_partition_cols.items()))
    #out_final_partition_cols = out_final_partition_cols if not(out_final_partition_cols) else out_final_partition_cols[0]
    out_final_repartition_no = sum(list(set(val for key, val in output_repartition_numbers.items())))


    ## Summarising File sizes starts
    out_final_number_of_dynamic_partitions = list(set(val for key, val in output_number_of_dynamic_partitions.items()))
    out_final_number_of_dynamic_partitions = 0 if not (
        out_final_number_of_dynamic_partitions) else out_final_number_of_dynamic_partitions[0]
    inp_final_file_sizes = {}
    out_final_file_size = ''

    for node_no, file_size in file_sizes.items():
        flag_correct = True
        file_size_trimmed = float(file_size.split(' ')[0].strip())
        if file_size.split(' ')[-1].strip() == 'B':
            file_size_in_gb = bytes_to_gb(file_size_trimmed)
        elif file_size.split(' ')[-1].strip() == 'KiB':
            file_size_in_gb = kb_to_gb(file_size_trimmed)

        elif file_size.split(' ')[-1].strip() == 'MiB':
            file_size_in_gb = mb_to_gb(file_size_trimmed)
        elif file_size.split(' ')[-1].strip() == 'TiB':
            file_size_in_gb = tb_to_gb(file_size_trimmed)
        elif file_size.split(' ')[-1].strip() == 'GiB':
            file_size_in_gb = file_size_trimmed

        else:
            custom_log(f"size is {file_size} -> It is neither in B, KiB, MiB, GiB or TiB. Hence keeping it as is")
            file_size_in_gb = file_size_trimmed
            flag_correct = False

        if flag_correct:
            file_size_in_gb = float(file_size_in_gb)

        if nodeID in inp_final_file_sizes:
            inp_final_file_sizes[nodeID].append(file_size_in_gb)
        else:
            inp_final_file_sizes[nodeID] = [file_size_in_gb]

    ##for file_size in output_size:

    if output_size:
        file_size = output_size[0]
        flag_correct = True
        file_size_trimmed = float(file_size.split(' ')[0].strip())
        if file_size.split(' ')[-1].strip() == 'B':
            file_size_in_gb = bytes_to_gb(file_size_trimmed)
        elif file_size.split(' ')[-1].strip() == 'KiB':
            file_size_in_gb = kb_to_gb(file_size_trimmed)

        elif file_size.split(' ')[-1].strip() == 'MiB':
            file_size_in_gb = mb_to_gb(file_size_trimmed)
        elif file_size.split(' ')[-1].strip() == 'TiB':
            file_size_in_gb = tb_to_gb(file_size_trimmed)
        elif file_size.split(' ')[-1].strip() == 'GiB':
            file_size_in_gb = file_size_trimmed

        else:
            custom_log(f"size is {file_size} -> It is neither in B, KiB, MiB, GiB or TiB. Hence keeping it as is")
            file_size_in_gb = file_size_trimmed
            flag_correct = False

        if flag_correct:
            file_size_in_gb = float(file_size_in_gb)

        out_final_file_size = file_size_in_gb


    if out_final_file_size == '':
        out_final_file_size = 0.00



        ## Summarising File sizes ends




    custom_log(f"inp_final_no_files -> {inp_final_no_files}")
    custom_log(f"inp_final_format_of_files -> {inp_final_format_of_files}")
    custom_log(f"inp_final_record_count -> {inp_final_record_count}")
    custom_log(f"inp_final_file_sizes  -> {inp_final_file_sizes}")
    custom_log(f"out_final_no_files -> {out_final_no_files}")
    custom_log(f"out_final_format_of_file  -> {out_final_format_of_file}")
    custom_log(f"out_final_record_count -> {out_final_record_count}")
    custom_log(f"out_final_path -> {out_final_path}")
    custom_log(f"out_final_partition_cols -> {out_final_partition_cols}")
    custom_log(f"out_final_repartition_no -> {out_final_repartition_no}")
    custom_log(f"out_final_file_size  -> {out_final_file_size}")
    custom_log(f"out_final_number_of_dynamic_partitions -> {out_final_number_of_dynamic_partitions}")
    custom_log(f"Total Duration is -> {out_final_duration} Minutes")



    ## Summarising Part Ends
    if out_final_no_files > threshold_out_final_no_files:
        custom_log(f"C_ERROR!!! The save in path {out_final_path} has created {out_final_no_files} number of files. The threshold set is {threshold_out_final_no_files}")

    ## Writing to dataframe path

    path_clean = out_final_path.replace("//", "/")
    path_split = path_clean.split('/')
    if len(path_split) < 2:
        custom_log(f"process_name not found in path {out_final_path}. Hence placing process_name as 'unknown'")
        process_name='unknown'
    else:
        process_name = path_split[2]


    data = (year, month, day, process_name,actual_sql_id, sql_status, {}, inp_final_format_of_files, inp_final_no_files, inp_final_file_sizes, inp_final_record_count,
                out_final_path,out_final_no_files, out_final_file_size,
            out_final_record_count, out_final_schema, out_final_format_of_file, out_final_number_of_dynamic_partitions,out_final_partition_cols, out_final_repartition_no,
                out_final_duration, dt )
    
    #utput_df.repartition(1).write.partitionBy("year", "month", "day").mode("append").parquet(f"{save_stats_s3_path}")
    #write_to_db(output_df)
    return data

        


## safe_spark calls this function
def spark_rest_api(spark):
    custom_log(f"Inside spark_rest_api")
    #time.sleep(10)
    final_stats_result = []
    try:
        url = generate_api_url(spark)
        response=spark_rest_api_call(url)
        final_response = response.json()
        custom_log(f"total number of elements in response is {len(final_response)}")
        
        for sql_id in range(len(final_response)):
            tmp_data=get_parameters_of_sql_id(final_response,sql_id )
            if tmp_data:
                final_stats_result.append(tmp_data)

        ## Display information  
        custom_log("Final result is")
        custom_log(final_stats_result)
        if not final_stats_result:
            custom_log(f"Entire resultset is Null meaning there were no write commands called in spark application")
            exit(0)
        output_df = pd.DataFrame(final_stats_result, columns=final_stats_columns)
        print(output_df)
        print(output_df[['dest_no_of_files','dest_size_in_gb','dest_count','dest_schema','dest_partition_cols','duration_in_mins','timestamp']].head(20))


    except Exception as e:
        custom_log(f"caught Exception at main spark_rest_api execution with exception -> {e}")



