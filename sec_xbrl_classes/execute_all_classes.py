from company_statements_xbrl import CompanyStatementsXBRL
from company_statements_json import CompanyStatementsJSON
from company_statements_csv import CompanyStatementCSV
from company_statements_timeseries import CompanyStatementTimeseries
from company_statements_standardize import CompanyStatementStandardize 
from company_stockrow_reconcilation import CompanyStockrowReconcilation 


from utils import setup_logging, upload_statement_files_and_logs 

from time import time
from datetime import datetime
import random

from google.cloud import storage

import os
import sys

def already_uploaded(bucket_name, ticker):

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)

    key_files = ['Income Statement.csv','Balance Sheet.csv','Cash Flow.csv']
    key_folder = f'{ticker}/Clean Statements/'

    key_files_exist = True
    for file in key_files:    
        stats = storage.Blob(bucket=bucket, name=f'{key_folder}{file}').exists(storage_client)

        if stats == False:

            key_files_exist = False
            break

    return key_files_exist




def xbrl_to_statement(ticker,data_path,overall_logger,bucket_name,update_only=True):
    if already_uploaded(bucket_name,ticker):
        overall_logger.info(f'{ticker} already uploaded')
    else:
    
        try:
            company_xbrl = CompanyStatementsXBRL(ticker,data_path,overall_logger,update_only)
        except:
            overall_logger.error('XBRL: {}. {}, line: {} in {}'.format(sys.exc_info()[0],sys.exc_info()[1],sys.exc_info()[2].tb_lineno,sys.exc_info()[2].tb_lineno))
        else:
            try:
                company_json = CompanyStatementsJSON(ticker,data_path,overall_logger,update_only)
            except:
                overall_logger.error('JSON: {}. {}, line: {} in {}'.format(sys.exc_info()[0],sys.exc_info()[1],sys.exc_info()[2].tb_lineno,sys.exc_info()[2].tb_lineno))
            else:
                try:
                    company_csv = CompanyStatementCSV(ticker,data_path,overall_logger,update_only)
                except:
                    overall_logger.error('CSV: {}. {}, line: {} in {}'.format(sys.exc_info()[0],sys.exc_info()[1],sys.exc_info()[2].tb_lineno,sys.exc_info()[2].tb_lineno))
                else:
                    try:
                        company_timeseries = CompanyStatementTimeseries(ticker,data_path,overall_logger,update_only)
                    except:
                        overall_logger.error('TIMESERIES: {}. {}, line: {} in {}'.format(sys.exc_info()[0],sys.exc_info()[1],sys.exc_info()[2].tb_lineno,sys.exc_info()[2].tb_lineno))
                    else:
                        try:
                            company_standard = CompanyStatementStandardize(ticker,data_path,overall_logger)
                        except:
                            overall_logger.error('STANDARDIZED: {}. {}, line: {} in {}'.format(sys.exc_info()[0],sys.exc_info()[1],sys.exc_info()[2].tb_lineno,sys.exc_info()[2].tb_lineno))
                        else:
                            try:
                                upload_statement_files_and_logs(bucket_name,data_path,'timeseries',ticker)
                                company_stockrow = CompanyStockrowReconcilation(ticker,data_path,overall_logger)
                            except:
                                overall_logger.error('RECONCILE: {}. {}, line: {} in {}'.format(sys.exc_info()[0],sys.exc_info()[1],sys.exc_info()[2].tb_lineno,sys.exc_info()[2].tb_lineno))

  

#



