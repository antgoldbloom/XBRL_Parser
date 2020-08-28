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

from google.cloud import pubsub_v1

project_id = "kaggle-playground-170215"
publisher = pubsub_v1.PublisherClient()


def download_xbrl(event, context):


    ticker, update_only, data_path, overall_logger, bucket_name = initialize_key_params(event)

    start_time = time()
    company_xbrl = CompanyStatementsXBRL(ticker,data_path,overall_logger,bucket_name,update_only)
    overall_logger.info(f"______{time()-start_time}______") 

    topic_id = "create_json_pubsub_topic"
    topic_name = 'projects/{project_id}/topics/{topic}'.format(project_id=project_id,topic=topic_id)
    publisher.publish(topic_name, data=f"{ticker} published".encode('utf-8'), ticker=ticker, update_only=event['attributes']['update_only'])

    print(f"download_xbrl for {ticker}: Success!")

def create_json(event, context):

    ticker, update_only, data_path, overall_logger, bucket_name = initialize_key_params(event)

    start_time = time()
    company_json = CompanyStatementsJSON(ticker,data_path,overall_logger,bucket_name,update_only)
    overall_logger.info(f"______{time()-start_time}______") 

    topic_id = "create_csv_pubsub_topic"
    topic_name = 'projects/{project_id}/topics/{topic}'.format(project_id=project_id,topic=topic_id)
    publisher.publish(topic_name, data=f"{ticker} published".encode('utf-8'), ticker=ticker, update_only=event['attributes']['update_only'])
    
    print(f"create_json for {ticker}: Success!")


def create_csv(event, context):

    ticker, update_only, data_path, overall_logger, bucket_name = initialize_key_params(event)

    start_time = time()
    company_csv = CompanyStatementCSV(ticker,data_path,overall_logger,bucket_name,update_only)
    overall_logger.info(f"______{time()-start_time}______") 

    topic_id = "create_timeseries_pubsub_topic"
    topic_name = 'projects/{project_id}/topics/{topic}'.format(project_id=project_id,topic=topic_id)
    publisher.publish(topic_name, data=f"{ticker} published".encode('utf-8'), ticker=ticker, update_only=event['attributes']['update_only'])
    
    print(f"create_csv for {ticker}: Success!")


def create_timeseries(event, context):

    ticker, update_only, data_path, overall_logger, bucket_name = initialize_key_params(event)

    start_time = time()
    company_timeseries = CompanyStatementTimeseries(ticker,data_path,overall_logger,bucket_name,update_only)
    company_standard = CompanyStatementStandardize(ticker,data_path,overall_logger,bucket_name)
    overall_logger.info(f"______{time()-start_time}______") 
    
    print(f"create_timeseries for {ticker}: Success!")




def initialize_key_params(event):

    data_path="/tmp/"
    Path(data_path).mkdir(parents=True, exist_ok=True)

    log_time = datetime.now().strftime('%Y_%m_%d__%H_%M_%S')
    overall_logger = setup_logging(f"{data_path}/logs/__OVERALL__/",f'{log_time}.log',f'error_{log_time}')
    bucket_name = 'kaggle-sec-data'

    ticker = event['attributes']['ticker']

    if event['attributes']['update_only'] == 't':
        update_only = True
    else: 
        update_only = False

    return ticker, update_only, data_path, overall_logger, bucket_name

