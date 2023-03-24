#Importing required Python dependencies.
import boto3
import botocore
import pandas as pd
from IPython.display import display, Markdown

#Setting up variables for S3 client and S3 resource
s3 = boto3.client('s3')
s3_resource = boto3.resource('s3')

#Define create_bucket function
def create_bucket(bucket):
    import logging

    try:
        s3.create_bucket(Bucket=bucket)
    except botocore.exceptions.ClientError as e:
        logging.error(e)
        return 'Bucket ' + bucket + ' could not be created.'
    return 'Created or already exists ' + bucket + ' bucket.'

#Call create_bucket function
create_bucket('nyctlc-cs653-5174')

#Importing data
def copy_among_buckets(from_bucket, from_key, to_bucket, to_key):
    s3_resource.meta.client.copy({'Bucket': from_bucket, 'Key': from_key}, 
                                    to_bucket, to_key)        

#Call copy_among_buckets function for cp yellow taxi rides Jan - Mar 2017
copy_among_buckets(from_bucket='nyc-tlc', from_key='trip data/yellow_tripdata_2017-01.paquet',
                      to_bucket='nyctlc-cs653-5174', to_key='trip data/yellow_tripdata_2017-01.paquet')
copy_among_buckets(from_bucket='nyc-tlc', from_key='trip data/yellow_tripdata_2017-02.paquet',
                      to_bucket='nyctlc-cs653-5174', to_key='trip data/yellow_tripdata_2017-02.paquet')
copy_among_buckets(from_bucket='nyc-tlc', from_key='trip data/yellow_tripdata_2017-03.paquet',
                      to_bucket='nyctlc-cs653-5174', to_key='trip data/yellow_tripdata_2017-03.paquet')




