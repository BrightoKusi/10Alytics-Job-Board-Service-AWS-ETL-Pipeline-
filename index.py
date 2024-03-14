import configparser 
import boto3 
import redshift_connector as rdc
import pandas as pd
import io
import json
import requests
import psycopg2

from utils.help import create_bucket, retrieve_api_data_and_upload_to_s3
from utils.constants import raw_file_path

config = configparser.ConfigParser()
config.read('.env')


['AWS']
access_key = config['AWS']['access_key']
secret_key = config['AWS']['secret_key']
raw_bucket_name = config['AWS']['raw_bucket_name']
transformed_bucket_name = config['AWS']['transformed_bucket_name']
region = config['AWS']['region']
role = config['AWS']['role']

['API']
api_key = config['API']['api_key']


#=====create an s3 bucket
create_bucket(access_key = access_key, secret_key = secret_key, bucket_name = raw_bucket_name, region = region)


#========= Load json file to bucket
retrieve_api_data_and_upload_to_s3(access_key, secret_key, api_key, raw_bucket_name, raw_file_path)


#========== Create new bucket for transformed data
create_bucket(access_key=access_key, secret_key=secret_key, bucket_name=transformed_bucket_name, region=region)
