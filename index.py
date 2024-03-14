import configparser 
import boto3 
import redshift_connector as rdc
import pandas as pd
import io
import json
import requests
import psycopg2

from utils.help import create_bucket


config = configparser.ConfigParser()
config.read('.env')


['AWS']
access_key = config['AWS']['access_key']
secret_key = config['AWS']['secret_key']
raw_bucket_name = config['AWS']['raw_bucket_name']
transformed_bucket_name = config['AWS']['transformed_bucket_name']
region = config['AWS']['region']
role = config['AWS']['role']



#=====create an s3 bucket
create_bucket(access_key, secret_key, raw_bucket_name, region)

