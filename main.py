import sys
sys.path.append('sec_xbrl_classes')

import requests
import csv
from datetime import datetime
from time import time
import os 
from utils import setup_logging 
from pathlib import Path

from execute_all_classes import xbrl_to_statement 

#os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="/Users/goldbloom/Dropbox/Side Projects/Edgar/Key/kaggle-playground-0f760ec0ebcd.json"

def create_company_statement(request):
    """HTTP Cloud Function.
    Args:
        request (flask.Request): The request object.
        <http://flask.pocoo.org/docs/1.0/api/#flask.Request>
    Returns:
        The response text, or any set of values that can be turned into a
        Response object using `make_response`
        <http://flask.pocoo.org/docs/1.0/api/#flask.Flask.make_response>.
    """


    request_json = request.get_json(silent=True)
    request_args = request.args

    if request_json and 'ticker' in request_json:
        ticker = request_json['ticker']
    elif request_args and 'ticker' in request_args:
        ticker = request_args['ticker']

    data_path="/tmp/data/"
    Path(data_path).mkdir(parents=True, exist_ok=True)

    log_time = datetime.now().strftime('%Y_%m_%d__%H_%M_%S')
    overall_logger = setup_logging(f"{data_path}/logs/__OVERALL__/",f'{log_time}.log',f'error_{log_time}')

    update_only = False 
    bucket_name = 'kaggle_sec_data'

    overall_logger.info(f'______{ticker}______')
    start_time = time()
    xbrl_to_statement(ticker,data_path,overall_logger,bucket_name,update_only)
    overall_logger.info(f"______{time()-start_time}______") 

    
    return f"{ticker} successfully processed"

