import logging
from pathlib import Path
from google.cloud import storage
import os

def setup_logging(log_folder,log_file,logger_name):
    logger = logging.getLogger(logger_name)
    logger.setLevel(logging.INFO)
    
    formatter = logging.Formatter("%(levelname)s - %(message)s") 

    Path(log_folder).mkdir(parents=True, exist_ok=True)
    file_handler = logging.FileHandler(f"{log_folder}{log_file}") 
    file_handler.setFormatter(formatter)

    stream_handler = logging.StreamHandler()

    logger.addHandler(file_handler)
    logger.addHandler(stream_handler)

    return logger

def upload_statement_files(bucket_name, data_path,output_type,ticker,logger):
    """Uploads a file to the bucket."""
    # bucket_name = "your-bucket-name"
    # source_file_name = "local/path/to/file"
    # destination_blob_name = "storage-object-name"
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    
    files_uploaded = []
    
    source_path = f'{data_path}data/{ticker}/{output_type}/'
    destination_blob_path = f'data/{ticker}/{output_type}/'

    for dirname, _, filenames in os.walk(source_path):
        for filename in filenames:

            if filename[-4:] in [".xml",".xsd","json",".csv"]:
                source_file_name = f'{dirname}/{filename}'

                if len(dirname) > len(source_path):
                    subdirs = f"{dirname[len(source_path):]}/"
                else:
                    subdirs = ""

                destination_blob_name = f'{destination_blob_path}{subdirs}{filename}'
    
                blob = bucket.blob(destination_blob_name)

                blob.upload_from_filename(source_file_name)

                files_uploaded.append(filename)


            
    if len(files_uploaded) > 0:
        logger.info(f"Files uploaded: {', '.join(files_uploaded)}")
    else:
        logger.error(f"No files uploaded")


def download_statement_files(bucket_name, data_path, output_type,ticker,logger): ##UP TO HERE
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
            
            destination__path_and_filename = f"{destination_root}{blob.name}"
            Path(destination__path_and_filename[:destination__path_and_filename.rfind('/')]).mkdir(parents=True, exist_ok=True)
            blob.download_to_filename(destination__path_and_filename)
            files_downloaded.append(blob.name)

    logger.info(f"Files downloaded: {', '.join(files_downloaded)}")

def delete_statement_files(bucket_name, data_path, output_type,ticker,logger): ##UP TO HERE
    """Deletes a blob from the bucket."""
    # bucket_name = "your-bucket-name"
    # blob_name = "your-object-name"

    storage_client = storage.Client()

    files_deleted = []

    bucket = storage_client.bucket(bucket_name)
    #blob = bucket.blob(source_blob_name)
    source_blob_path = f'data/{ticker}/{output_type}'

    for blob in bucket.list_blobs(prefix=source_blob_path):
        blob.delete()
        files_deleted.append(blob.name) 

    logger.info(f"Files deleted: {', '.join(files_deleted)}")


def upload_log_files(bucket_name, data_path,ticker,logger):

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    
    files_uploaded = []
    
    source_log_path = f'{data_path}logs/{ticker}/'
    destination_log_path = f'logs/{ticker}/'

    for dirname, _, filenames in os.walk(source_log_path):
        for filename in filenames:

            if filename[-4:] == ".log":
                source_file_name = f'{dirname}/{filename}'

                subdirs = f"{dirname[len(source_log_path):]}/"
                destination_blob_name = f'{destination_log_path}{subdirs}{filename}'
            
                blob = bucket.blob(destination_blob_name)
                blob.upload_from_filename(source_file_name)

                files_uploaded.append(filename)

    logger.info(f"Files uploaded: {', '.join(files_uploaded)}")