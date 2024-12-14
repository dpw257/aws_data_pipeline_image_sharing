
from json import loads
from kafka import KafkaConsumer
from multiprocessing import Process
from sqlalchemy import text
from time import sleep
import json
import random
import requests
import sqlalchemy



invoke_url = "https://v303yghy09.execute-api.us-east-1.amazonaws.com/129076a9eaf9-http-proxy"



class AWSDBConnector:

    def __init__(self):

        self.HOST = "pinterestdbreadonly.cq2e8zno855e.eu-west-1.rds.amazonaws.com"
        self.USER = 'project_user'
        self.PASSWORD = ':PASSWORD'
        self.DATABASE = 'pinterest_data'
        self.PORT = 3306
        
    def create_db_connector(self):
        '''
        Connects to Pintrest data on AWS.
        '''
        engine = sqlalchemy.create_engine(f"mysql+pymysql://{self.USER}:{self.PASSWORD}@{self.HOST}:{self.PORT}/{self.DATABASE}?charset=utf8mb4")
        return engine


new_connector = AWSDBConnector()

def send_data_to_topic(topic, payload):
    '''
    Sends pintrest data to Kafka topics.
    Attributes:
    topic (string): topic name 
    payload (json): dictionary of values to add as data row
    '''
    # Send a POST request to create a new resource
    headers = {'Content-Type': 'application/vnd.kafka.json.v2+json'}
    response = requests.post(f"{invoke_url}/topics/129076a9eaf9.{topic}", headers=headers, data=payload)
    # Check if the request was successful (status code 201)
    if response.status_code == 201:
        print("New resource created successfully")
    else:
        print(f"Request failed with status code: {response.status_code}")

def data_consumer(topic):
    '''
    Reports status of upload to Kafka.
    Attributes:
    topic (string): topic name 
    '''
    data_stream_consumer = KafkaConsumer(
    bootstrap_servers="b-2.pinterestmskcluster.w8g8jt.c12.kafka.us-east-1.amazonaws.com:9098,b-3.pinterestmskcluster.w8g8jt.c12.kafka.us-east-1.amazonaws.com:9098,b-1.pinterestmskcluster.w8g8jt.c12.kafka.us-east-1.amazonaws.com:9098",    
    value_deserializer=lambda message: loads(message),
    auto_offset_reset="earliest" 
    )
    data_stream_consumer.subscribe(topics=[{topic}])
    for message in data_stream_consumer:
        print(message.value)
        print(message.topic)
        print(message.timestamp)


def run_post_data_loop():
    '''
    Downloads data from AWS as json file.
    '''
    for value in range(1000):
        sleep(random.randrange(0, 1))
        random_row = random.randint(0, 11000)
        engine = new_connector.create_db_connector()
        with engine.connect() as connection:

            pin_string = text(f"SELECT * FROM pinterest_data LIMIT {random_row}, 1")
            pin_selected_row = connection.execute(pin_string)
            for row in pin_selected_row:
                pin_result = dict(row._mapping)
                pin_payload = json.dumps({
                    "records": [
                        {      
                        "value": {"index": pin_result["index"], "unique_id": pin_result["unique_id"], "title": pin_result["title"], "description": pin_result["description"], "poster_name": pin_result["poster_name"], "follower_count": pin_result["follower_count"], "tag_list": pin_result["tag_list"], "is_image_or_video": pin_result["is_image_or_video"], "image_src": pin_result["image_src"], "downloaded": pin_result["downloaded"], "save_location": pin_result["save_location"], "category": pin_result["category"]}
                        }
                    ]
                })
            send_data_to_topic('pin', pin_payload)
            data_consumer('pin')


            geo_string = text(f"SELECT * FROM geolocation_data LIMIT {random_row}, 1")
            geo_selected_row = connection.execute(geo_string)
            for row in geo_selected_row:
                geo_result = dict(row._mapping)
                geo_result['timestamp'] = geo_result['timestamp'].isoformat()
                geo_payload = json.dumps({
                    "records": [
                        {      
                        "value": {"ind": geo_result["ind"], "timestamp": geo_result["timestamp"], "latitude": geo_result["latitude"], "longitude": geo_result["longitude"], "country": geo_result["country"]}
                        }
                    ]
                })
            send_data_to_topic('geo', geo_payload)
            data_consumer('geo')


            user_string = text(f"SELECT * FROM user_data LIMIT {random_row}, 1")
            user_selected_row = connection.execute(user_string)
            for row in user_selected_row:
                user_result = dict(row._mapping)
                user_result['date_joined'] = user_result['date_joined'].isoformat()
                user_payload = json.dumps({
                    "records": [
                        {      
                        "value": {"ind": user_result["ind"], "first_name": user_result["first_name"], "last_name": user_result["last_name"], "age": user_result["age"], "date_joined": user_result["date_joined"]}
                        }
                    ]
                })
            send_data_to_topic('user', user_payload)
            data_consumer('user')




if __name__ == "__main__":
    run_post_data_loop()
    print('Working')
