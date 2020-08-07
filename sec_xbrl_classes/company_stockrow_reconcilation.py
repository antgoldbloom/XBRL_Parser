import pandas as pd
import os

from datetime import datetime, date, timedelta
from time import time
from dateutil.relativedelta import *

import shutil

import numpy as np
import math
from collections import Counter

import calendar
from pathlib import Path

from utils import setup_logging 

import json

import requests
import urllib.parse

import sys


class CompanyStockrowReconcilation: 

    def __init__(self,ticker,data_path,overall_logger):

        self.ticker = ticker 
        self.log_path = f"{data_path}logs/{ticker}/{overall_logger.name[6:]}/"
        self.timeseries_path = f"{data_path}timeseries/"
        self.stockrow_path = f"{data_path}stockrow/{ticker}"


        reconcile_logger = setup_logging(self.log_path,'.log',f'reconcile_{ticker}')
        reconcile_logger.info(f'_____{ticker}_TIMESERIES_STANDARDIZED_____')

        self.download_stock_row_statements(ticker,data_path,overall_logger,reconcile_logger)

        for statement in ['Income Statement','Balance Sheet', 'Cash Flow']:
            overall_logger.info(f'___{statement}___')
            reconcile_logger.info(f'___{statement}___')

            df, df_sr = self.load_statement(data_path,ticker,statement)
            missing_labels, missing_labels_in_stock_row = self.missing_rows(data_path,df,df_sr,statement)
            if len(missing_labels) > 0:
                overall_logger.warning(f"\nmissing labels: {', '.join(missing_labels)}")
                overall_logger.info(f"missing labels in stockrow: {', '.join(missing_labels_in_stock_row)}\n")
                reconcile_logger.warning(f"\nmissing labels: {', '.join(missing_labels)}")
                reconcile_logger.info(f"missing labels in stockrow: {', '.join(missing_labels_in_stock_row)}\n")


            missing_dates = self.missing_columns(df,df_sr)
            if len(missing_dates) > 0:
                overall_logger.warning(f"missing dates: {', '.join(missing_dates)}\n")
                reconcile_logger.warning(f"missing dates: {', '.join(missing_dates)}\n")

            label_discrepancy_dict = self.label_discrepancies(df,df_sr)

            for label in label_discrepancy_dict:
                if len(label_discrepancy_dict[label]) > 0:
                    overall_logger.warning(label_discrepancy_dict[label].T)
                    reconcile_logger.warning(label_discrepancy_dict[label].T)



    def download_stock_row_statements(self,ticker,data_path,overall_logger,reconcile_logger):
        Path(self.stockrow_path).mkdir(parents=True, exist_ok=True)



        for statement in ['Income Statement','Cash Flow','Balance Sheet']:

            if not os.path.exists(f'{self.stockrow_path}/{statement}.xlsx'): 
                url = f'https://stockrow.com/api/companies/{ticker}/financials.xlsx?dimension=Q&section={urllib.parse.quote(statement)}&sort=desc'
                try:
                    r = requests.get(url, allow_redirects=True)
                except: 
                    overall_logger.error(f'Failed to download {statement} for {ticker}')
                    reconcile_logger.error(f'Failed to download {statement} for {ticker}')


                if (b'The page you were looking for doesn\'t exist (404)' in r.content):
                    overall_logger.error(f"{statement}.xlsx' not found") 
                    reconcile_logger.error(f"{statement}.xlsx' not found") 
                else:
                    open(f'{self.stockrow_path}/{statement}.xlsx', 'wb').write(r.content)


    def lower_index(self,df):
        df.index = df.index.str.lower()
        return df      

    def load_statement(self,data_path,ticker,statement):
        df = pd.read_csv(f"{data_path}timeseries/{ticker}/Cleaned Statements/{statement}.csv",index_col=[2])  
        df = df.drop(['filing_label','xbrl_tag'],axis=1)
        df.columns = self.list_of_rounded_months(df.columns)
        df.columns = pd.to_datetime(df.columns)

        df = self.lower_index(df)

        df_sr = pd.read_excel(f"{data_path}stockrow/{ticker}/{statement}.xlsx",index_col=[0])  
        df_sr = self.lower_index(df_sr)

        return df, df_sr

    def match_rows_and_columns(self,df,df_sr):
        matched_labels = df[df.index.isin(df_sr.index)].index
        matched_dates = df.loc[:,df.columns.isin(df_sr.columns.astype(str))].columns
        
        df = df.loc[matched_labels,matched_dates]
        df_sr = df_sr.loc[matched_labels,matched_dates]
        df_sr.index = [f'{index}_stockrow' for index in df_sr.index]

        return df,df_sr

    def missing_rows(self,data_path,df,df_sr,statement):
        with open(f"{data_path}/mappings/canonical_label_tag_mapping.json") as json_file:
            mapping_dict = json.load(json_file)
            
        canonical_keys = [key.lower() for key in mapping_dict[statement].keys()]

        missing_rows = pd.Index(canonical_keys)[~pd.Index(canonical_keys).isin(df.index)]
        missing_rows_in_stock_row = df_sr[df_sr.index.isin(missing_rows)].index
        
        return missing_rows, missing_rows_in_stock_row

    def missing_columns(self,df,df_sr):
        missing_dates = df_sr.columns[~df_sr.columns.isin(df.columns)]
        return [datetime.strftime(date,'%Y-%m-%d') for date in missing_dates]
        
    def find_discrepancy(self,df,df_sr,label):

        df_matched, df_sr_matched = self.match_rows_and_columns(df,df_sr)
        not_matched = df_matched.loc[label,:] != df_sr_matched.loc[f'{label}_stockrow',:]

        return pd.DataFrame(df_matched.loc[label,not_matched]).merge(df_sr_matched.loc[f'{label}_stockrow',not_matched],left_index=True,right_index=True)

    def label_discrepancies(self,df,df_sr):
        label_dict = {}
        for label in df.index:
            if (label is not np.nan) and (label in df.index) and (label in df_sr.index):
                label_dict[label] = self.find_discrepancy(df,df_sr,label)
        return label_dict

    #for label in (df_matched == df_sr_matched).all(axis=1):

    def load_stockrow_statement(self,ticker,data_path,statement_type,stockrow_logger):
        statement_folder = f'{data_path}stockrow/{ticker}/'
        df_comparison = pd.read_excel(f'{statement_folder}{statement_type}.xlsx',index_col=[0])
        df_comparison.columns = pd.DatetimeIndex(df_comparison.columns)
        return df_comparison
                    
    def list_of_rounded_months(self,date_list):
        rounded_date_list = []

        for date_str in date_list:

            rounded_date = self.date_round(date_str)
            rounded_date_list.append(rounded_date)

        return rounded_date_list


    def date_round(self,date_str):
        year = int(date_str[0:4])

        month = int(date_str[5:7])
        day = int(date_str[8:10])

        if round(day/calendar.monthrange(year,month)[1],0) == 1:
            rounded_date = (date(year, month, 1) + relativedelta(months=+1) - timedelta(days=1))
        else:
            rounded_date = (date(year, month, 1) - timedelta(days=1))

        return rounded_date



#data_path = '../data/'
#log_time = datetime.now().strftime('%Y_%m_%d__%H_%M_%S')
#update_only = False 

#overall_logger = setup_logging(f"{data_path}/logs/__OVERALL__/",f'{log_time}.log',f'error_{log_time}')

#for ticker in os.listdir(f'{data_path}/timeseries/'): 
#    overall_logger.info(f'______{ticker}______')
#    try:
#        company_stockrow = CompanyStockrowReconcilation(ticker,data_path,overall_logger)
#    except:
#        overall_logger.error('RECONCILE: {}. {}, line: {} in {}'.format(sys.exc_info()[0],sys.exc_info()[1],sys.exc_info()[2].tb_lineno,sys.exc_info()[2].tb_lineno))

    

