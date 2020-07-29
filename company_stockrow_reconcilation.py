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

    def __init__(self,ticker,data_path,log_time_folder,refresh=False):

        self.ticker = ticker 
        self.log_path = f"{data_path}logs/{ticker}/{log_time_folder}/"
        self.timeseries_path = f"{data_path}timeseries/"
        self.stockrow_path = f"{data_path}stockrow/{ticker}"

        start_time = time()

        if refresh:
            shutil.rmtree(f"{self.stockrow_path}", ignore_errors=True, onerror=None)  #remove if exists 


        stockrow_logger = setup_logging(self.log_path,'stockrow.log',f'stockrow_{ticker}')
        stockrow_logger.info(f'_____{ticker}_STOCKROW_____')

        statements_downloaded = self.download_stock_row_statements(ticker,data_path,stockrow_logger)

        
        #df = {}

        self.m_dict = {}

        if statements_downloaded:

            for statement in ['Income Statement','Cash Flow','Balance Sheet']:

                #df[statement] = match_statement_and_rename_labels(ticker, statement,data_path)
                stockrow_logger.info(f'____{statement}____')
                self.m_dict[statement] = self.match_statement_and_rename_labels(ticker, statement,data_path,stockrow_logger)


                #df[statement] = fill_missing_from_stockrow(df[statement],ticker,data_path)

            stockrow_logger.info(f"Time: {time()-start_time}")



    def download_stock_row_statements(self,ticker,data_path,stockrow_logger):
        stockrow_folder = f'{data_path}stockrow/{ticker}/'
        Path(stockrow_folder).mkdir(parents=True, exist_ok=True)

        downloaded = True


        for statement in ['Income Statement','Cash Flow','Balance Sheet']:

            if os.path.exists(f'{stockrow_folder}/{statement}.xlsx'): 
                stockrow_logger.info(f"Didn't download {statement} for {ticker} because file already exists")
            else: 
                url = f'https://stockrow.com/api/companies/{ticker}/financials.xlsx?dimension=Q&section={urllib.parse.quote(statement)}&sort=desc'
                try:
                    r = requests.get(url, allow_redirects=True)
                except: 
                    stockrow_logger.error(f'Failed to download {statement} for {ticker}')


                if (b'The page you were looking for doesn\'t exist (404)' in r.content):
                    stockrow_logger.warning(f"{statement}.xlsx' not found") 
                    downloaded = False 
                else:
                    open(f'{stockrow_folder}/{statement}.xlsx', 'wb').write(r.content)

        return downloaded



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

    def adjust_timeseries_for_match(self,df_timeseries,df_comparison,stockrow_logger):
        df_timeseries = df_timeseries.loc[df_timeseries[df_timeseries.columns.max()].notna(),:] #delete rows with na for latest period
        rounded_date_list = self.list_of_rounded_months(df_timeseries.columns)
        df_timeseries.columns = pd.DatetimeIndex(rounded_date_list)

        #if the same date appears twice (e.g. BA 2019-12-31 Consolidated Statements Of Financial Position, then keep the column with the fewest NAs)
        duplicated_dates = np.array([k for k,v in Counter(rounded_date_list).items() if v>1])
        for date in duplicated_dates: 
            duplicate_index = np.where(df_timeseries.columns.isin([date]))[0] 
            column_to_keep = duplicate_index[np.argmax(list(df_timeseries.iloc[:,duplicate_index].notna().sum()))]
            for index in duplicate_index:
                if index != column_to_keep: 
                    #can't use drop because it will drop both columns
                    column_indexes = [x for x in range(df_timeseries.shape[1])]  
                    column_indexes.remove(index) 
                    df_timeseries = df_timeseries.iloc[:,column_indexes]



        #[k for k,v in Counter(mylist).items() if v>1]
        #df_timeseries.columns = df_timeseries.columns)

        
        #df_timeseries.index = df_timeseries.index
        #df_timeseries = df_timeseries.loc[:,df_timeseries.columns.isin(df_comparison.columns)]/1000000
        return df_timeseries

    def match_statement(self,df_timeseries,df_comparison,stockrow_logger):
        tmp_mapping_dict = {}

        for comparison_row in df_comparison.index:
            
            #print(f"To Match: {comparison_row}")

            
            for timeseries_row in df_timeseries.index:
                #print(timeseries_row)
                if (timeseries_row[1][:8] == 'us-gaap_') and ('___' not in timeseries_row[1]):
                    
                    

                    
                    s_timeseriesrow_not_na = df_timeseries.loc[timeseries_row,df_timeseries.columns.isin(df_comparison.columns)].dropna() 
                    s_comparisonrow_matchdates = df_comparison.loc[comparison_row,df_comparison.columns.isin(s_timeseriesrow_not_na.index)]
                    s_match = s_timeseriesrow_not_na == s_comparisonrow_matchdates
                    match_pct = s_match.sum()/s_match.count()

                    if match_pct == 1:
                        #print(f"MATCH: {timeseries_row[0]}")
                        #uncomment when using renaming
                        #tmp_mapping_dict = add_to_mapping_dict(tmp_mapping_dict,timeseries_row[0],comparison_row)
                        tmp_mapping_dict = self.add_to_mapping_dict(tmp_mapping_dict,timeseries_row[1],comparison_row)
                            
                    elif match_pct > 0.8:
                        stockrow_logger.warning(f'Only found a {round(match_pct*100,1)}% match between {timeseries_row[1]} and {comparison_row}')
                        #uncomment when using renaming
                        #tmp_mapping_dict = add_to_mapping_dict(tmp_mapping_dict,timeseries_row[0],comparison_row)
                        tmp_mapping_dict = self.add_to_mapping_dict(tmp_mapping_dict,timeseries_row[1],comparison_row)
        
        return tmp_mapping_dict


                        
    def add_to_mapping_dict(self,tmp_mapping_dict,ts_row_0,comparison_row):
        if ts_row_0 not in tmp_mapping_dict:
            tmp_mapping_dict[ts_row_0] = []

        tmp_mapping_dict[ts_row_0].append(comparison_row)

        return tmp_mapping_dict

    def rename_index_labels(self,df_timeseries,matched_mapping_dict,matched_statement,timeseries_statement_folder):
        df_timeseries = pd.read_csv(os.path.join(timeseries_statement_folder,matched_statement),index_col=[0,1])
        df_timeseries = df_timeseries.rename(index=matched_mapping_dict,level=0)
        return df_timeseries

    def match_statement_and_rename_labels(self,ticker, statement,data_path,stockrow_logger):
        

        df_comparison = self.load_stockrow_statement(ticker,data_path,statement,stockrow_logger)
        #print(f"{statement} has {len(df_comparison)} rows")
        timeseries_statement_folder = f'{data_path}timeseries/{ticker}/Statement/'


        max_overlap = 0
        
        matched_mapping_dict = None

        for statement_csv in os.listdir(timeseries_statement_folder):
            df_timeseries = pd.read_csv(f'{timeseries_statement_folder}{statement_csv}',index_col=[0,1])
            
            df_timeseries = self.adjust_timeseries_for_match(df_timeseries,df_comparison,stockrow_logger)
            tmp_mapping_dict = self.match_statement(df_timeseries,df_comparison,stockrow_logger)

            if len(tmp_mapping_dict.keys()) > max_overlap:
                max_overlap = len(tmp_mapping_dict.keys())
                matched_statement = statement_csv 
                matched_mapping_dict = tmp_mapping_dict

        if matched_mapping_dict is not None:
            for key in matched_mapping_dict:
                stockrow_logger.info(f"{key}: {', '.join(matched_mapping_dict[key])}")
                #print(matched_mapping_dict[key])
            #else: 
            #    stockrow_logger.error(f"No match for {ticker}'s {statement}")
            
        
        
        #df_timeseries = rename_index_labels(df_timeseries,matched_mapping_dict,matched_statement,timeseries_statement_folder)

            
        #return df_timeseries 
        return matched_mapping_dict 

    def fill_missing_from_stockrow(self,df,ticker,data_path,stockrow_logger):

        df_stockrow = pd.read_excel(f'{data_path}stockrow/{ticker}/{statement}.xlsx',index_col=[0])

        month_end_columns = self.list_of_rounded_months(df.columns)
        #print(month_end_columns)
        #[datetime.strptime(f"{date_col[0:4]}-{date_col[5:7]}-{calendar.monthrange(int(date_col[0:4]),int(date_col[5:7]))[1]}",'%Y-%m-%d') for date_col in df[statement].columns]

        for row in df.loc[df.index.get_level_values(0).isin(df_stockrow.index),:].index:
            #print(row)
            for column in df.loc[:,pd.DatetimeIndex(month_end_columns).isin(df_stockrow.columns)]:
                #print(column)
                rounded_column = self.date_round(column)
                if (math.isnan(df.loc[row,column])) and (row[0] in df_stockrow.index) and (rounded_column in df_stockrow.columns):
                    df.loc[row,column] = df_stockrow.loc[row[0],rounded_column]
                    stockrow_logger.info(f"Filled in [{row[0]},{column}] from stockrow")
        
        return df



data_path = '../data/'
log_time_folder = datetime.now().strftime('%Y_%m_%d__%H_%M_%S')
update_only = False 

#with open(f"{data_path}mappings/ticker_tag_label_mapping.json") as json_file:
#    m_dict = json.load(json_file)

for ticker in os.listdir(f'{data_path}/timeseries/'): 
    try:
        company_stockrow = CompanyStockrowReconcilation(ticker,data_path,log_time_folder,update_only)
    except:
        print('JSON: {}. {}, line: {} in {}'.format(sys.exc_info()[0],sys.exc_info()[1],sys.exc_info()[2].tb_lineno,sys.exc_info()[2].tb_lineno))
    else:
        #m_dict[ticker] = company_stockrow.m_dict 
        #with open(f"{data_path}mappings/ticker_tag_label_mapping.json", 'w') as json_file:
            #json.dump(m_dict, json_file)
        
    
