from zipfile import ZipFile 
import os 
from google.cloud import storage
os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="/Users/goldbloom/Dropbox/Side Projects/Edgar/Key/kaggle-playground-0f760ec0ebcd.json"
  
def get_all_file_paths(file_paths,directory): 
  
    # crawling through directory and subdirectories 
    for filename in os.listdir(directory): 
            # join the two strings in order to form the full filepath. 
            if filename[-4:] != "zip": 
                filepath = os.path.join(directory, filename) 
                file_paths.append(filepath) 
  
    # returning all file paths 
    return file_paths         
  
def zip_function_files(zip_file): 
    # path to folder which needs to be zipped 
    
    if os.path.exists(zip_file):
        os.remove(zip_file)
    # calling function to get all file paths in the directory 
    file_paths = [] 
    file_paths = get_all_file_paths(file_paths,'./') 
    file_paths = get_all_file_paths(file_paths,'./sec_xbrl_classes') 
    file_paths = get_all_file_paths(file_paths,'./mapping') 
  
    # writing files to a zipfile 
    with ZipFile(zip_file,'w') as zip: 
        # writing each file one by one 
        for file in file_paths: 
            zip.write(file) 
  
    print('All files zipped successfully!')         

def upload_to_gcs(bucket_name,upload_directory,zip_file):
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)

    destination_blob_name = f"{upload_directory}/{zip_file}"
        
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(zip_file)
    os.remove(zip_file)
    print('files uploaded to GCS')
  
gcs_upload_folder = 'cloud functions' 
zip_file = 'sec_downloader_functions.zip'
bucket_name = 'kaggle_sec_data' 
zip_function_files(zip_file)
upload_to_gcs(bucket_name ,gcs_upload_folder,zip_file)

gcloud_command = f"/Users/goldbloom/google-cloud-sdk/bin/gcloud functions deploy create_kaggle_sec_data --entry-point create_company_statement --runtime python37 --trigger-http --timeout 540 --region us-west2 --memory 1024MB --source 'gs://{bucket_name}/{gcs_upload_folder}/{zip_file}'"
stream = os.popen(gcloud_command)




  