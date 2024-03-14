import boto3
import io
import pandas as pd
import json
import configparser 
import requests
import redshift_connector as rdc
import psycopg2

config = configparser.ConfigParser()
config.read('.env')


['AWS']
access_key = config['AWS']['access_key']
secret_key = config['AWS']['secret_key']
raw_bucket_name = config['AWS']['raw_bucket_name']
transformed_bucket_name = config['AWS']['transformed_bucket_name']
region = config['AWS']['region']
role = config['AWS']['role']



def create_bucket(access_key, secret_key, bucket_name, region):
    '''
    Description:
        This function creates an Amazon S3 bucket in the specified AWS region using the provided access key, secret key, bucket name, and region information.

    Parameters:
        - access_key (str, optional): The AWS access key ID used for authentication.
        - secret_key (str, optional): The AWS secret access key used for authentication.
        - bucket_name (str, optional): The name for the S3 bucket to be created.
        - region (str, optional): The AWS region where the bucket will be created.

    Returns:
        - None
    '''
    # Initialize Boto3 S3 client
    client = boto3.client(
        's3',
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key
    )
    
    # Create bucket in specified region
    try:
        response = client.create_bucket(
            Bucket=bucket_name,
            CreateBucketConfiguration={
                'LocationConstraint': region,
            }
        )
        print(f"Bucket '{bucket_name}' created successfully in region '{region}'.")
    except Exception as e:
        print(f"An error occurred:", e)

