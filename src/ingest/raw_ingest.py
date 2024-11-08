import requests
import xmltodict
import json
import os
import boto3
from datetime import datetime
from dotenv import load_dotenv

API_ENDPOINT = os.load_dotenv("API_ENDPOINT")
AWS_ACCESS_KEY = os.load_dotenv("AWS_ACCESS_KEY")
AWS_SECRET_KEY = os.load_dotenv("AWS_SECRET_KEY")

# Create serviceresource object to connect to s3
s3 = boto3.resource(
    service_name='s3',
    region_name='us-east-1',
    aws_access_key_id = AWS_ACCESS_KEY,
    aws_secret_access_key = AWS_SECRET_KEY
)

# Ingest data
    
response = requests.get(API_ENDPOINT, headers={'Accept': 'application/xml'})

# check to make sure request went through
if response.status_code == 200:
    predict_xml = response.text
    # convert xml to dict
    python_dict = xmltodict.parse(predict_xml)
    # convert dict to json string
    json_string = json.dumps(python_dict)
    # place in s3 bucket
    today_date = datetime.today()
    # Format the date as 'mmddyyyy'
    formatted_date = today_date.strftime('%m%d%Y')
    # load into s3 bucket
    s3.Object('predictit-de-project', f'raw/predictit_data_raw{formatted_date}.json').put(Body=json_string)

else:
    print("Request was unsuccessful")


