
from google.cloud import storage

import os
from pathlib import Path
import shutil
from time import time
from datetime import datetime

import subprocess
from zipfile import ZipFile,BadZipfile

import numpy as np

import logging

debug = True##REVERT 




def setup_logging(log_folder,log_file,logger_name):
    logger = logging.getLogger(logger_name)
    logger.setLevel(logging.INFO)
    
    formatter = logging.Formatter("%(levelname)s - %(message)s") 

    Path(log_folder).mkdir(parents=True, exist_ok=True)
    file_handler = logging.FileHandler("{}{}".format(log_folder,log_file)) 
    file_handler.setFormatter(formatter)

    stream_handler = logging.StreamHandler()

    logger.addHandler(file_handler)
    logger.addHandler(stream_handler)

    return logger


def list_gcs_directories(client, bucket, prefix,debug=False):
    # from https://github.com/GoogleCloudPlatform/google-cloud-python/issues/920
    iterator = client.list_blobs(bucket, prefix=prefix, delimiter='/')
    prefixes = set()
    for page in iterator.pages:
        prefixes.update(page.prefixes)
        if debug:
            break #only do one page
    return prefixes


def download_statement_files(storage_client, bucket, data_path,ticker): ##UP TO HERE

    files_downloaded = []

    source_blob_path = 'data/{}/timeseries/'.format(ticker)

    download_count = 0
    for blob in bucket.list_blobs(prefix=source_blob_path):
        if blob.name[-4:] == ".csv":
            
            destination_path_and_filename = "{}{}".format(data_path,blob.name).replace('timeseries/','')
            Path(destination_path_and_filename[:destination_path_and_filename.rfind('/')]).mkdir(parents=True, exist_ok=True)
            blob.download_to_filename(destination_path_and_filename)
            files_downloaded.append(blob.name)
            download_count += 1

    return download_count


def create_archive_logger(data_path):
    log_time = datetime.now().strftime('%Y_%m_%d__%H_%M_%S')
    archive_log_name = 'archive_{}.log'.format(log_time)
    return setup_logging(data_path,archive_log_name,'archive_logger') 


bucket_name = 'kaggle-sec-data'
storage_client = storage.Client()
bucket = storage_client.bucket(bucket_name)

if debug == True:
    data_path = '/Users/goldbloom/Dropbox/Side Projects/Edgar/data/tmp/'
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="/Users/goldbloom/Dropbox/Side Projects/Edgar/Key/kaggle-playground-0f760ec0ebcd.json"
    ticker_list = ['AMZN']
    archive_logger = create_archive_logger(data_path)
else:
    data_path = './'
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="./kaggle-playground-0f760ec0ebcd.json"
    ticker_list = list_gcs_directories(storage_client,bucket,'data/',debug)
    ticker_list = [ticker[5:-1] for ticker in ticker_list]
    archive_logger = create_archive_logger(data_path)
    archive_logger.info("{} Tickers in GCS".format(len(ticker_list)))


zip_path = '{}archive/'.format(data_path) 
Path(zip_path).mkdir(parents=True, exist_ok=True)
zip_filename = 'sec_filing.zip'
zip_path_and_filename = '{}{}'.format(zip_path,zip_filename)

try:
    sec_zip = ZipFile(zip_path_and_filename)
    files_to_del = [folder for folder in sec_zip.namelist() if folder[:folder.find('/')] in ticker_list]
    if (len(files_to_del) > 0):
        cmd=['zip', '-d', zip_path_and_filename] + files_to_del
        subprocess.check_call(cmd,stdout=open(os.devnull, 'wb'))
except BadZipfile:
    os.remove(zip_path_and_filename) 
    archive_logger.warning("Existing {} was bad. Deleting.".format(zip_filename))
except FileNotFoundError:
    archive_logger.warning("{} not found. Will create".format(zip_filename))
else: 
    tickers_to_del = np.unique([ticker[:ticker.find('/')] for ticker in files_to_del])
    archive_logger.info("Deleting files in {} tickers".format(len(tickers_to_del)))
ticker_count = 0

no_statement_list = []


start_time = time()
with ZipFile(zip_path_and_filename, 'a') as sec_zip:
    for ticker in ticker_list:
        download_count = download_statement_files(storage_client,bucket, data_path, ticker) 
        timeseries_path = '{}data/{}/'.format(data_path,ticker)
        if download_count > 0:
            for root_path, directories, filenames in os.walk(timeseries_path):
                for filename in filenames:
                    directory = root_path[root_path.rfind('/')+1:]
                    sec_zip.write(os.path.join(root_path, filename),'/{}/{}/{}'.format(ticker,directory,filename))

            ticker_count += 1 
            if ticker_count % 100 == 0:
                print("Processed {} tickers (up to {}; last 100 done in {} seconds)".format(ticker_count,ticker,time()-start_time))
                start_time = time()
        else:
            no_statement_list.append(ticker) 

        shutil.rmtree({timeseries_path}, ignore_errors=True, onerror=None)  #remove if exists - this is fast so haven't bothered with adding update only functionality:w



archive_logger.warning("No Statements Found for the Following Tickers")
for ticker in no_statement_list:
    archive_logger.warning(ticker)
    

archive_logger.info("{} ticker(s) on GCS; {} with no statements".format(len(ticker_list),len(no_statement_list)))


archive_blob_name = 'archive/sec_filing.zip'
blob = bucket.blob(archive_blob_name)
blob.upload_from_filename(zip_path_and_filename)

log_blob_name = 'archive/logs/{}'.format(archive_log_name)
blob = bucket.blob(log_blob_name)
blob.upload_from_filename('{}{}'.format(data_path,archive_log_name))


ds_store = '{}.DS_Store'.format(zip_path)
if os.path.exists(ds_store):
    os.remove(ds_store)
cmd=['kaggle', 'datasets', 'version', '-p', zip_path,'-m','Updating'] 
subprocess.check_call(cmd)