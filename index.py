import configparser 
import boto3 
import redshift_connector as rdc
import pandas as pd
import io
import json
import requests
import psycopg2

from utils.help import create_bucket, retrieve_api_data_and_upload_to_s3, transform_and_upload_to_s3
from utils.constants import raw_file_path, transformed_file_path, dev_schema
from sql_statements.create import transformed_jobs_data

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

['DWH']
dwh_host = config['DWH']['host']
dwh_user = config['DWH']['user']
dwh_password = config['DWH']['password']
dwh_database = config['DWH']['database']


#=====create an s3 bucket
create_bucket(access_key = access_key, secret_key = secret_key, bucket_name = raw_bucket_name, region = region)


#========= Load json file to bucket
retrieve_api_data_and_upload_to_s3(access_key, secret_key, api_key, raw_bucket_name, raw_file_path)


#========== Create new bucket for transformed data
create_bucket(access_key=access_key, secret_key=secret_key, bucket_name=transformed_bucket_name, region=region)


#========== copy data from raw_data_bucket, transform and load to transformed_data_bucket
transform_and_upload_to_s3(access_key, secret_key, raw_bucket_name, raw_file_path, transformed_bucket_name, transformed_file_path)


#============ Create connection to data warehouse using redshift_connector
conn_details = {'host':dwh_host, 'database': dwh_database, 'user':dwh_user, 'password': dwh_password}
dwh_conn = rdc.connect(**conn_details)
cursor = dwh_conn.cursor()
print('connection successful')


#============= Create a schema for the transformed jobs in Redshift warehouse
query  = f'CREATE SCHEMA IF NOT EXISTS {dev_schema}'
cursor.execute(query)
dwh_conn.commit()
print('created')


#========================== Create the table for the database in the dev schema
query = transformed_jobs_data
print(f'----------------------------{query[:50]}')
cursor.execute(query)
dwh_conn.commit()
print('created succesfully')