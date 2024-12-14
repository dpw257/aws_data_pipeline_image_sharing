import requests
from time import sleep
import random
from multiprocessing import Process
import boto3
import json
from json import loads
import sqlalchemy
from sqlalchemy import text
from kafka import KafkaConsumer




random.seed(100)

pin_invoke_url = "https://v303yghy09.execute-api.us-east-1.amazonaws.com/129076a9eaf9-http-proxy/streams/streaming-129076a9eaf9-pin/record"
geo_invoke_url = "https://v303yghy09.execute-api.us-east-1.amazonaws.com/129076a9eaf9-http-proxy/streams/streaming-129076a9eaf9-geo/record"
user_invoke_url = "https://v303yghy09.execute-api.us-east-1.amazonaws.com/129076a9eaf9-http-proxy/streams/streaming-129076a9eaf9-user/record"



class AWSDBConnector:

    def __init__(self):

        self.HOST = "pinterestdbreadonly.cq2e8zno855e.eu-west-1.rds.amazonaws.com"
        self.USER = 'project_user'
        self.PASSWORD = ':t%;yCY3Yjg'
        self.DATABASE = 'pinterest_data'
        self.PORT = 3306
        
    def create_db_connector(self):
        '''
        Connects to Pintrest stream on AWS.
        '''
        engine = sqlalchemy.create_engine(f"mysql+pymysql://{self.USER}:{self.PASSWORD}@{self.HOST}:{self.PORT}/{self.DATABASE}?charset=utf8mb4")
        return engine


new_connector = AWSDBConnector()

def send_data_to_topic(payload, invoke_url):
    '''
    Sends pintrest data to Kafka topics.
    Attributes:
    payload (json): dictionary of values to add as data row.
    invoke_url (string): url to AWS resource to save data to.
    '''
    headers = {'Content-Type': 'application/json'}
    response = requests.request("PUT", invoke_url, headers=headers, data=payload)
    # Check if the request was successful (status code 201)
    if response.status_code == 201:
        print("New resource created successfully")
    else:
        print(f"Request failed with status code: {response.status_code}")


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
                    "StreamName": "streaming-129076a9eaf9-pin", 
                    "Data": {
                        "index": pin_result["index"], "unique_id": pin_result["unique_id"], "title": pin_result["title"], "description": pin_result["description"], "poster_name": pin_result["poster_name"], "follower_count": pin_result["follower_count"], "tag_list": pin_result["tag_list"], "is_image_or_video": pin_result["is_image_or_video"], "image_src": pin_result["image_src"], "downloaded": pin_result["downloaded"], "save_location": pin_result["save_location"], "category": pin_result["category"]
                        }, 
                        "PartitionKey": "partition-0"
                        })
            send_data_to_topic(pin_payload, pin_invoke_url)
            
            geo_string = text(f"SELECT * FROM geolocation_data LIMIT {random_row}, 1")
            geo_selected_row = connection.execute(geo_string)
            for row in geo_selected_row:
                geo_result = dict(row._mapping)
                geo_result['timestamp'] = geo_result['timestamp'].isoformat()
                geo_payload = json.dumps({
                    "StreamName": "streaming-129076a9eaf9-geo", 
                    "Data": {  
                        "ind": geo_result["ind"], "timestamp": geo_result["timestamp"], "latitude": geo_result["latitude"], "longitude": geo_result["longitude"], "country": geo_result["country"]
                        }, 
                        "PartitionKey": "partition-0"
                        })
            send_data_to_topic(geo_payload, geo_invoke_url)

            user_string = text(f"SELECT * FROM user_data LIMIT {random_row}, 1")
            user_selected_row = connection.execute(user_string)
            for row in user_selected_row:
                user_result = dict(row._mapping)
                user_result['date_joined'] = user_result['date_joined'].isoformat()
                user_payload = json.dumps({
                    "StreamName": "streaming-129076a9eaf9-geo", 
                    "Data": {   
                        "data": {"ind": user_result["ind"], "first_name": user_result["first_name"], "last_name": user_result["last_name"], "age": user_result["age"], "date_joined": user_result["date_joined"]}
                        }, 
                        "PartitionKey": "partition-0"
                        })
            send_data_to_topic(user_payload, pin_invoke_url)




if __name__ == "__main__":
    run_post_data_loop()
    print('Working')
