import sys
sys.path.append('sec_xbrl_classes')


from google.cloud import storage

import os
from pathlib import Path
import py7zr
import shutil


os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="/Users/goldbloom/Dropbox/Side Projects/Edgar/Key/kaggle-playground-0f760ec0ebcd.json"

bucket_name = 'kaggle_sec_data'

storage_client = storage.Client()
bucket = storage_client.bucket(bucket_name)

#def extract_prefixes(bucket, prefix=None, delimiter='/'):
#    iterator = bucket.list_blobs(prefix=prefix, delimiter=delimiter)
#    map(lambda x: None, iterator)
#    return iterator.prefixes


def list_gcs_directories(client, bucket, prefix,debug=False):
    # from https://github.com/GoogleCloudPlatform/google-cloud-python/issues/920
    iterator = client.list_blobs(bucket, prefix=prefix, delimiter='/')
    prefixes = set()
    for page in iterator.pages:
        #print (page, page.prefixes)
        prefixes.update(page.prefixes)
        if debug:
            break
    return prefixes


def download_statement_files(bucket_name, data_path, output_type,ticker,logger=None): ##UP TO HERE
    """Downloads a blob from the bucket."""
    # bucket_name = "your-bucket-name"
    # source_blob_name = "storage-object-name"
    # destination_file_name = "local/path/to/file"

    storage_client = storage.Client()

    files_downloaded = []

    bucket = storage_client.bucket(bucket_name)
    #blob = bucket.blob(source_blob_name)
    source_blob_path = f'data/{ticker}/{output_type}/'
    destination_root = f'{data_path}'

    for blob in bucket.list_blobs(prefix=source_blob_path):
        if blob.name[-4:] in [".xml",".xsd","json",".csv"]:
            
            destination__path_and_filename = f"{destination_root}{blob.name}".replace('timeseries/','')
            Path(destination__path_and_filename[:destination__path_and_filename.rfind('/')]).mkdir(parents=True, exist_ok=True)
            blob.download_to_filename(destination__path_and_filename)
            files_downloaded.append(blob.name)

ticker_list = list_gcs_directories(storage_client,bucket,'data/',True)
ticker_list = [ticker[5:-1] for ticker in ticker_list]

with py7zr.SevenZipFile('target.7z', 'w') as z:
    for ticker in ticker_list:
        data_path = '../data/tmp/'
        output_type = 'timeseries'
        download_statement_files(bucket_name, data_path, output_type,ticker) 
        timeseries_path = f'{data_path}data/{ticker}/'
        z.writeall(timeseries_path)
        shutil.rmtree(f"{timeseries_path}", ignore_errors=True, onerror=None)  #remove if exists - this is fast so haven't bothered with adding update only functionality:w

#iterator = bucket.list_blobs(delimiter='/', prefix='data/')
#response = iterator._get_next_page_response()
#for prefix in response['prefixes']:
    #print('gs://'+bucket_name+'/'+prefix)
#    print(prefix[5:-1])

#    download_statement_files  
#    download_statement_files(bucket_name, data_path, output_type,ticker,logger):