
from google.cloud import storage

import os
from pathlib import Path
import shutil
from time import time
from datetime import datetime

import subprocess
from zipfile import ZipFile,BadZipfile

import requests
import csv

import numpy as np

import logging


def list_gcs_directories(client, bucket, prefix,debug=False):
    # from https://github.com/GoogleCloudPlatform/google-cloud-python/issues/920
    iterator = client.list_blobs(bucket, prefix=prefix, delimiter='/')
    prefixes = set()
    for page in iterator.pages:
        prefixes.update(page.prefixes)
        if debug:
            break #only do one page
    ticker_list = [ticker[5:-1] for ticker in prefixes]
    return ticker_list 



def setup_bucket():  
    bucket_name = 'kaggle-sec-data'
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    return bucket_name, storage_client, bucket

                    
def get_all_tickers():
    resp = requests.get('https://www.sec.gov/include/ticker.txt')
    ticker_cik_text = resp.content.decode('utf-8')
    ticker_cik_list = list(csv.reader(ticker_cik_text.splitlines(), delimiter='\t'))
    
    ticker_list = []
    for item in ticker_cik_list:
        ticker_list.append(item[0].upper())

    return ticker_list


debug = True

if debug == True:
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="/Users/goldbloom/Dropbox/Side Projects/Edgar/Key/kaggle-playground-0f760ec0ebcd.json"
else:
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="./kaggle-playground-0f760ec0ebcd.json"

bucket_name, storage_client, bucket = setup_bucket()

sec_ticker_list = get_all_tickers() 

gcs_ticker_list = list_gcs_directories(storage_client,bucket,'data/',False)

complete_ticker_list = []

for ticker in gcs_ticker_list:
    to_match = [f'{ticker} Income Statement.csv', f'{ticker} Balance Sheet.csv', f'{ticker} Cash Flow.csv']
    blobs = storage_client.list_blobs(bucket_name, prefix=f'data/{ticker}/timeseries/Clean Statements/')
    for blob in blobs:    
        filename_and_path = blob.name
        filename = filename_and_path[filename_and_path.rfind('/')+1:] 
        if filename in to_match:
            to_match.remove(filename)


    if len(to_match) == 0:
        complete_ticker_list.append(ticker)


print("Missing from GCS list")
gcs_missing = [ticker for ticker in sec_ticker_list if ticker not in gcs_ticker_list] 
print(gcs_missing)
print("\n")

print("Not complete in GCS")
gcs_not_complete = [ticker for ticker in gcs_ticker_list if ticker not in complete_ticker_list] 
print(gcs_not_complete)
print("\n")

    
print(f"SEC: {len(sec_ticker_list)}; GCS: {len(gcs_ticker_list)}; GCS complete: {len(complete_ticker_list)}")    
print(f"Missing from GCS: {len(gcs_missing)}; Not complete in GCS: {len(gcs_not_complete)}")

