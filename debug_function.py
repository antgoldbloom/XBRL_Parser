import sys
sys.path.append('sec_xbrl_classes')


from main import download_xbrl, create_json, create_csv, create_timeseries
import json
import os



#os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="/Users/goldbloom/Dropbox/Side Projects/Edgar/Key/kaggle-playground-0f760ec0ebcd.json"
os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="/Users/goldbloom/Dropbox/Side Projects/Edgar/Key/kaggle-playground-0f760ec0ebcd.json"

request = {"ticker": "ZM",'update_only':'f'}

if download_xbrl(request,True):
    if create_json(request, True):
        if create_csv(request, True):
            if create_timeseries(request,True):
                print(f"{request['ticker']} success!") 



