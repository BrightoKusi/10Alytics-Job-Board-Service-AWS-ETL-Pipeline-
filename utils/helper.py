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


def retrieve_api_data_and_upload_to_s3(access_key, secret_key, api_key, raw_bucket_name, raw_file_path):
    try:
        url = "https://jsearch.p.rapidapi.com/search"
        merged_json = {}  # Initialize an empty dictionary to store merged data

        query_strings = [
            {"query": "Data Engineer or Data Analyst in USA", "page": "1", "num_pages": "10", "date_posted": "today"},
            {"query": "Data Engineer or Data Analyst in Canada", "page": "1", "num_pages": "10", "date_posted": "today"},
            {"query": "Data Engineer or Data Analyst in UK", "page": "1", "num_pages": "10", "date_posted": "today"}
        ]
        
        # Dictionary to store data for each country
        country_data = {}

        for querystring in query_strings:
            headers = {"X-RapidAPI-Key": api_key, "X-RapidAPI-Host": 'jsearch.p.rapidapi.com'}
            response = requests.get(url, headers=headers, params=querystring)
            
            if response.status_code == 200:
                data = response.json()  # Get the JSON response as a dictionary
                country_name = querystring['query'].split()[-1]  # Extract country name from query
                
                # Store data for each country separately
                country_data[country_name] = data
                
        # Convert country-wise data to JSON strings and upload to S3
        for country, data in country_data.items():
            json_data = json.dumps(data)
            
            # Upload JSON data to S3 bucket with country-specific file paths
            s3 = boto3.client('s3', aws_access_key_id=access_key, aws_secret_access_key=secret_key)
            s3.put_object(Bucket=raw_bucket_name, Key=f"{raw_file_path}_{country}.json", Body=json_data)
            print(f'API data retrieved as {raw_file_path}_{country}.json and loaded into {raw_bucket_name}')
        
    except Exception as e:
        print('There was an error: ', e)