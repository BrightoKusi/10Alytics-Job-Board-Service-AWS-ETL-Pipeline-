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


['DWH']
dwh_host = config['DWH']['host']
dwh_user = config['DWH']['user']
dwh_password = config['DWH']['password']
dwh_database = config['DWH']['database']


source_bucket = raw_bucket_name
destination_bucket = transformed_bucket_name

from utils.constants import raw_file_path, transformed_file_path, dev_schema, transformed_table


def create_bucket(access_key = None, secret_key = None, bucket_name = None, region = None):
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




def transform_and_upload_to_s3(access_key, secret_key, raw_bucket_name, raw_file_path, transformed_bucket_name, transformed_file_path):
    try:
        # Establishing a connection to Amazon S3
        s3 = boto3.client('s3', aws_access_key_id=access_key, aws_secret_access_key=secret_key)

        # Retrieving JSON data from the specified S3 bucket and file
        response = s3.get_object(Bucket=raw_bucket_name, Key=raw_file_path)
        data = response['Body'].read().decode('utf-8')

        # Processing each line of the data as JSON and constructing a list of JSON objects
        all_data = [json.loads(line) for line in data.split('\n') if line.strip()]
    

        # Creating a DataFrame from the list of JSON objects
        df = pd.DataFrame(all_data)
        
        # Assuming 'data' key is present in each dictionary within all_data list
        df = pd.concat([pd.json_normalize(entry['data']) for entry in all_data], ignore_index=True)

        # Filtering and renaming columns
        desired_columns = ['job_id', 'job_employment_type', 'job_title', 'job_apply_link',
                           'job_description', 'job_city', 'job_country', 'job_posted_at_timestamp',
                           'employer_website', 'employer_company_type']
        df = df[desired_columns]

        # Convert Unix timestamp column to datetime
        df['job_posted_at_timestamp'] = pd.to_datetime(df['job_posted_at_timestamp'], unit='s')
        
        # Convert DataFrame to CSV format in memory
        csv_buffer = io.StringIO()
        df.to_csv(csv_buffer, index=False)

        # Initialize S3 client for destination bucket
        s3_dest = boto3.client('s3', aws_access_key_id=access_key, aws_secret_access_key=secret_key)

        # Upload CSV data to the destination S3 bucket
        s3_dest.put_object(Bucket=transformed_bucket_name, Key=transformed_file_path, Body=csv_buffer.getvalue().encode('utf-8'))

        print("Data transformed and uploaded to S3 as CSV")
    except Exception as e:
        print('Error:', e)




def copy_from_s3_to_redshift():
#==Create connection to redshift
    conn_details = {'host':dwh_host, 'database': dwh_database, 'user':dwh_user, 'password': dwh_password}
    dwh_conn = rdc.connect(**conn_details)
    cursor = dwh_conn.cursor()
    print('connection successful')
    
    #Query for copying from s3 to redshift
    query = f'''
            COPY {dev_schema}.{transformed_table}
            FROM 's3://{transformed_bucket_name}/{transformed_file_path}'
            IAM_ROLE '{role}'
            DELIMITER ',' 
            IGNOREHEADER 1
            FORMAT AS CSV;
        '''
    try:
        cursor.execute(query)
        dwh_conn.commit()
        print('Data copied successfully')
    except Exception as e:
        print(e)
        cursor.close()
        dwh_conn.close()













