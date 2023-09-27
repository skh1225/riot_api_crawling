import json
import boto3
from google.cloud import storage
from config import config
import json
import os

os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="/Users/skh/Documents/workspace/project/lol_crawler/config/fourth-way-398009-b0ce29a3bf47.json"

storage_client = storage.Client()

bucket_name = 'summoner-match'

s3_conn = boto3.resource('s3', **config.s3_connection)
bucket = s3_conn.Bucket(bucket_name)

for obj in bucket.objects.all():
  key = obj.key
  bucket = storage_client.bucket(bucket_name)
  blob = bucket.blob(key)
  blob.upload_from_string(data=obj.get()['Body'].read().decode('UTF-8'),content_type='application/json')
