import sys
sys.path.append('sec_xbrl_classes')

import requests
import csv
from datetime import datetime
from time import time
import os 
from utils import setup_logging 
from pathlib import Path

from company_statements_xbrl import CompanyStatementsXBRL
from company_statements_json import CompanyStatementsJSON
from company_statements_csv import CompanyStatementCSV
from company_statements_timeseries import CompanyStatementTimeseries
from company_statements_standardize import CompanyStatementStandardize 
from company_stockrow_reconcilation import CompanyStockrowReconcilation 


def download_xbrl(request, debug=False):

    ticker, update_only, data_path, overall_logger, bucket_name = initialize(request,debug)

    start_time = time()
    company_xbrl = CompanyStatementsXBRL(ticker,data_path,overall_logger,bucket_name,update_only)
    overall_logger.info(f"______{time()-start_time}______") 

    
    return True 

def create_json(request, debug=False):

    ticker, update_only, data_path, overall_logger, bucket_name = initialize(request,debug)

    start_time = time()
    company_json = CompanyStatementsJSON(ticker,data_path,overall_logger,bucket_name,update_only)
    overall_logger.info(f"______{time()-start_time}______") 
    
    return True 


def create_csv(request, debug=False):

    ticker, update_only, data_path, overall_logger, bucket_name = initialize(request,debug)

    start_time = time()
    company_csv = CompanyStatementCSV(ticker,data_path,overall_logger,bucket_name,update_only)
    overall_logger.info(f"______{time()-start_time}______") 
    
    return True 


def create_timeseries(request, debug=False):

    ticker, update_only, data_path, overall_logger, bucket_name = initialize(request,debug)

    start_time = time()
    company_timeseries = CompanyStatementTimeseries(ticker,data_path,overall_logger,bucket_name,update_only)
    company_standard = CompanyStatementStandardize(ticker,data_path,overall_logger,bucket_name)
    overall_logger.info(f"______{time()-start_time}______") 
    
    return True 




def initialize(request,debug=False):

    if debug == True:
        request_json = request
    else:
        request_json = request.get_json(silent=True)
        #request_args = request.args

    if (request_json or debug) and ('ticker' in request_json) and ('update_only' in request_json):
        ticker = request_json['ticker']
        if request_json['update_only'] == 't':
            update_only = True
        else: 
            update_only = False 



    #elif request_args and 'ticker' in request_args:
        #ticker = request_args['ticker']


    data_path="/tmp/"
    Path(data_path).mkdir(parents=True, exist_ok=True)

    log_time = datetime.now().strftime('%Y_%m_%d__%H_%M_%S')
    overall_logger = setup_logging(f"{data_path}/logs/__OVERALL__/",f'{log_time}.log',f'error_{log_time}')
    bucket_name = 'kaggle_sec_data'

    return ticker, update_only, data_path, overall_logger, bucket_name

