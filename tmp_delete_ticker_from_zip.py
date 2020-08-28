

import subprocess
from zipfile import ZipFile,BadZipfile
import os
import numpy as np

data_path = '/Users/goldbloom/Dropbox/Side Projects/Edgar/data/tmp/'

#ticker_list = ["HOFT","NWPP","FSRVU","FSRV","FSRVW"]

#zip_path = '{}archive/'.format(data_path) 
folder_path = '{}archive/sec_filing/'.format(data_path) 




#unzip_path = '{}archive/'.format(data_path) 
#Path(zip_path).mkdir(parents=True, exist_ok=True)
#zip_filename = 'sec_filing.zip'
#zip_path_and_filename = '{}{}'.format(zip_path,zip_filename)

#sec_zip = ZipFile(zip_path_and_filename)
#file_list = [f[:f.find('/')] for f in sec_zip.namelist() if f[f.rfind('/')+1:][:1] == ' ']
#file_name_list = [f[f.rfind('/')+1:] for f in sec_zip.namelist()] 

count = 0

for directory, _, files in os.walk(folder_path):
    #print(directory)
    #for filename in files
    #print(filename)
    ticker = directory[len(folder_path):directory.rfind('/')]
    if ticker != '':
        for filename in files:
            os.rename(os.path.join(directory,filename),os.path.join(directory,f'{ticker} {filename}'))

