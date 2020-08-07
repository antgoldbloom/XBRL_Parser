import logging
from pathlib import Path
from google.cloud import storage

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

def upload_statement_files_and_logs(bucket_name, data_path,output_type,ticker):
    """Uploads a file to the bucket."""
    # bucket_name = "your-bucket-name"
    # source_file_name = "local/path/to/file"
    # destination_blob_name = "storage-object-name"
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    
    files_uploaded = []
    
    source_path = f'{data_path}{output_type}/{ticker}/'
    destination_path = f'/data/{ticker}/{output_type}'

    source_log_path = f'{data_path}logs/{ticker}/'
    destination_log_path = f'/logs/{ticker}/'
    
    
    for dirname, _, filenames in os.walk(source_path):
        for filename in filenames:

            source_file_name = f'{dirname}/{filename}'
            destination_blob_name = f'{destination_path}{filename}'
        
    
            blob = bucket.blob(destination_blob_name)

            blob.upload_from_filename(source_file_name)

            files_uploaded.append(filename)

    for dirname, _, filenames in os.walk(source_log_path):
        for filename in filenames:

            source_file_name = f'{dirname}/{filename}'
            destination_blob_name = f'{destination_log_path}{filename}'
            
            blob = bucket.blob(destination_blob_name)
            blob.upload_from_filename(source_file_name)

            files_uploaded.append(filename)

            
    print(f"Files uploaded: {', '.join(files_uploaded)}")

