################################ Uploaded by Praveen ###########################################################
# Produces random taxi related info and produces that data into kafka topic called 'taxi_trips'
################################################################################################################


from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
import random
import json
import random
import time
from uuid import uuid4
from datetime import datetime, timedelta



# Kafka configuration (default settings)
KAFKA_BROKER = 'localhost:9092'
KAFKA_TOPIC = 'taxi_trips'

# Initialize Faker

# Payment types and companies for random selection
PAYMENT_TYPES = ['Credit Card', 'Cash', 'No Charge', 'Dispute', 'Unknown']
COMPANIES = ['Alpha Cabs', 'Beta Taxi', 'City Rides', 'Urban Mobility', 'Metro Taxi']


TAXI_ID_POOL_SIZE = 100
TAXI_IDS = [str(uuid4()) for _ in range(TAXI_ID_POOL_SIZE)]


default_args = {
    'owner': 'admin',
    'start_date': datetime(2023, 9, 3, 10, 00)
}

def random_datetime_within_last_30_days():
    now = datetime.now()
    start = now - timedelta(days=30)
    # Get a random number of seconds between start and now
    random_seconds = random.randint(0, int((now - start).total_seconds()))
    return start + timedelta(seconds=random_seconds)


def get_data():
    trip_start = random_datetime_within_last_30_days()
    trip_seconds = random.randint(60, 3600)
    trip_end = trip_start + timedelta(seconds=trip_seconds)
    trip_miles = round(random.uniform(0.5, 30.0), 2)
    fare = round(random.uniform(5.0, 100.0), 2)
    tips = round(random.uniform(0.0, 20.0), 2)
    tolls = round(random.uniform(0.0, 10.0), 2)
    extras = round(random.uniform(0.0, 5.0), 2)
    trip_total = round(fare + tips + tolls + extras, 2)
    pickup_lat = round(random.uniform(41.6, 42.1), 6)
    pickup_lon = round(random.uniform(-87.9, -87.5), 6)
    dropoff_lat = round(random.uniform(41.6, 42.1), 6)
    dropoff_lon = round(random.uniform(-87.9, -87.5), 6)
    pickup_area = random.randint(1, 77)
    dropoff_area = random.randint(1, 77)
    pickup_tract = round(random.uniform(8000, 9000), 1)
    dropoff_tract = round(random.uniform(8000, 9000), 1)
    now = datetime.now()

    return {
        "trip_id": str(uuid4()),
        "taxi_id": random.choice(TAXI_IDS),
        "trip_start_timestamp": trip_start.isoformat(),
        "trip_end_timestamp": trip_end.isoformat(),
        "trip_seconds": trip_seconds,
        "trip_miles": trip_miles,
        "pickup_census_tract": pickup_tract,
        "dropoff_census_tract": dropoff_tract,
        "pickup_community_area": pickup_area,
        "dropoff_community_area": dropoff_area,
        "fare": fare,
        "tips": tips,
        "tolls": tolls,
        "extras": extras,
        "trip_total": trip_total,
        "payment_type": random.choice(PAYMENT_TYPES),
        "company": random.choice(COMPANIES),
        "pickup_centroid_latitude": pickup_lat,
        "pickup_centroid_longitude": pickup_lon,
        "pickup_centroid_location": f"({pickup_lat}, {pickup_lon})",
        "dropoff_centroid_latitude": dropoff_lat,
        "dropoff_centroid_longitude": dropoff_lon,
        "dropoff_centroid_location": f"({dropoff_lat}, {dropoff_lon})",
        "event_timestamp": now.isoformat()
    }

def format_data(res):
    data = {}
    '''location = res['location']
    data['first_name'] = res['name']['first']
    data['last_name'] = res['name']['last']
    data['gender'] = res['gender']
    data['address'] = f"{str(location['street']['number'])} {location['street']['name']}, " \
                      f"{location['city']}, {location['state']}, {location['country']}"
    data['post_code'] = location['postcode']
    data['email'] = res['email']'''
    data['username'] = res['username']
    '''data['dob'] = res['dob']['date']
    data['registered_date'] = res['registered']['date']
    data['phone'] = res['phone']
    data['picture'] = res['picture']['medium']'''

    return data

def stream_data():
    import json
    from kafka import KafkaProducer
    import time
    import logging

    producer = KafkaProducer(bootstrap_servers='broker:9092', max_block_ms=5000)
    curr_time = time.time()

    while True:
        if time.time() > curr_time + 920: #2 minutes
            break
        try:
            res = get_data()
            ##res = format_data(res)

            producer.send(KAFKA_TOPIC, json.dumps(res).encode('utf-8'))
            sleep_duration=0.2
            time.sleep(sleep_duration)
        except Exception as e:
            logging.error(f'An error occured: {e}')
            continue

with DAG('produce_taxi_data',
         default_args=default_args,
         schedule_interval='@daily',
         catchup=False) as dag:

    streaming_task = PythonOperator(
        task_id='stream_data_from_api',
        python_callable=stream_data
    )
