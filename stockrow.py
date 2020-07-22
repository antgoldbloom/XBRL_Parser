import pandas as pd
import os

from datetime import datetime, date, timedelta
from time import time
from dateutil.relativedelta import *

import numpy as np
import math
from collections import Counter

import calendar
from pathlib import Path


import json

import requests
import urllib.parse

import quandl
quandl.ApiConfig.api_key = "ca9bMvozbsfJ41tQECsN"

pd.set_option('max_columns',250)
pd.set_option('max_rows',100)

class CompanyStockrowReconcilation: 

def download_stock_row_statements(ticker,data_path):
    stockrow_folder = f'{data_path}stockrow/{ticker}/'
    Path(stockrow_folder).mkdir(parents=True, exist_ok=True)

    for statement in ['Income Statement','Cash Flow','Balance Sheet']:
        url = f'https://stockrow.com/api/companies/{ticker}/financials.xlsx?dimension=Q&section={urllib.parse.quote(statement)}&sort=desc'
        try:
            r = requests.get(url, allow_redirects=True)
            open(f'{stockrow_folder}/{statement}.xlsx', 'wb').write(r.content)
        except: 
            print(f'Failed to download {statement} for {ticker}')

def load_stockrow_statement(ticker,data_path,statement_type):
    statement_folder = f'{data_path}stockrow/{ticker}/'
    df_comparison = pd.read_excel(f'{statement_folder}{statement_type}.xlsx',index_col=[0])
    df_comparison.columns = pd.DatetimeIndex(df_comparison.columns)
    return df_comparison
            
def list_of_rounded_months(date_list):
        rounded_date_list = []
        
        for date_str in date_list:

            rounded_date = date_round(date_str)
            rounded_date_list.append(rounded_date)
                
        return rounded_date_list
        

def date_round(date_str):
    year = int(date_str[0:4])
    
    month = int(date_str[5:7])
    day = int(date_str[8:10])

    if round(day/calendar.monthrange(year,month)[1],0) == 1:
        rounded_date = (date(year, month, 1) + relativedelta(months=+1) - timedelta(days=1))
    else:
        rounded_date = (date(year, month, 1) - timedelta(days=1))

    return rounded_date

def adjust_timeseries_for_match(df_timeseries,df_comparison):
    df_timeseries = df_timeseries.loc[df_timeseries[df_timeseries.columns.max()].notna(),:] #delete rows with na for latest period
    rounded_date_list = list_of_rounded_months(df_timeseries.columns)
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

def match_statement(df_timeseries,df_comparison):
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
                    tmp_mapping_dict = add_to_mapping_dict(tmp_mapping_dict,timeseries_row[1],comparison_row)
                        
                elif match_pct > 0.8:
                    print(f'Warning: only found a {round(match_pct*100,1)}% match between {timeseries_row[1]} and {comparison_row}')
                    #uncomment when using renaming
                    #tmp_mapping_dict = add_to_mapping_dict(tmp_mapping_dict,timeseries_row[0],comparison_row)
                    tmp_mapping_dict = add_to_mapping_dict(tmp_mapping_dict,timeseries_row[1],comparison_row)
    
    return tmp_mapping_dict


                    
def add_to_mapping_dict(tmp_mapping_dict,ts_row_0,comparison_row):
    if ts_row_0 not in tmp_mapping_dict:
        tmp_mapping_dict[ts_row_0] = []

    tmp_mapping_dict[ts_row_0].append(comparison_row)

    return tmp_mapping_dict

def rename_index_labels(df_timeseries,matched_mapping_dict,matched_statement):
    df_timeseries = pd.read_csv(os.path.join(timeseries_statement_folder,matched_statement),index_col=[0,1])
    df_timeseries = df_timeseries.rename(index=matched_mapping_dict,level=0)
    return df_timeseries

def match_statement_and_rename_labels(ticker, statement,data_path):
    

    df_comparison = load_stockrow_statement(ticker,data_path,statement)
    #print(f"{statement} has {len(df_comparison)} rows")
    
    max_overlap = 0
    
    for statement_csv in os.listdir(timeseries_statement_folder):
        df_timeseries = pd.read_csv(f'{timeseries_statement_folder}{statement_csv}',index_col=[0,1])
        
        df_timeseries = adjust_timeseries_for_match(df_timeseries,df_comparison)
        tmp_mapping_dict = match_statement(df_timeseries,df_comparison)

        if len(tmp_mapping_dict.keys()) > max_overlap:
            max_overlap = len(tmp_mapping_dict.keys())
            matched_statement = statement_csv 
            matched_mapping_dict = tmp_mapping_dict

    for key in matched_mapping_dict:
        if len(matched_mapping_dict[key]) > 1: 
            print(f"{key}: {', '.join(matched_mapping_dict[key])}")
            #print(matched_mapping_dict[key])
    
    
    #df_timeseries = rename_index_labels(df_timeseries,matched_mapping_dict,matched_statement)

        
    #return df_timeseries 
    return matched_mapping_dict 

def fill_missing_from_stockrow(df,ticker,data_path):

    df_stockrow = pd.read_excel(f'{data_path}stockrow/{ticker}/{statement}.xlsx',index_col=[0])

    month_end_columns = list_of_rounded_months(df.columns)
    #print(month_end_columns)
    #[datetime.strptime(f"{date_col[0:4]}-{date_col[5:7]}-{calendar.monthrange(int(date_col[0:4]),int(date_col[5:7]))[1]}",'%Y-%m-%d') for date_col in df[statement].columns]

    for row in df.loc[df.index.get_level_values(0).isin(df_stockrow.index),:].index:
        #print(row)
        for column in df.loc[:,pd.DatetimeIndex(month_end_columns).isin(df_stockrow.columns)]:
            #print(column)
            rounded_column = date_round(column)
            if (math.isnan(df.loc[row,column])) and (row[0] in df_stockrow.index) and (rounded_column in df_stockrow.columns):
                df.loc[row,column] = df_stockrow.loc[row[0],rounded_column]
                print(f"Filled in [{row[0]},{column}] from stockrow")
    
    return df



data_path = '../data/'
ticker_list = ['AAPL','AXP','BA','CAT','CVX','DD','GS','IBM','INTC','JPM','KO','MMM','MRK','MSFT','PFE','PG','CSCO', 'DIS','GE','HD','JNJ','MCD', 'NKE']
m_dict = {}

#for ticker in ticker_list[0:2]:

for ticker in os.listdir(f'{data_path}timeseries/'):
    print(ticker)
    start_time = time()

    download_stock_row_statements(ticker,data_path)
    timeseries_statement_folder = f'{data_path}timeseries/{ticker}/Statement/'
    
    df = {}
    m_dict[ticker] = {}

    for statement in ['Income Statement','Cash Flow','Balance Sheet']:

        #df[statement] = match_statement_and_rename_labels(ticker, statement,data_path)
        m_dict[ticker][statement] = match_statement_and_rename_labels(ticker, statement,data_path)

        with open(f"{data_path}logs/multi_match.json", 'w') as stock_json:
            json.dump(m_dict, stock_json)
        #df[statement] = fill_missing_from_stockrow(df[statement],ticker,data_path)

    print(f"Time: {time()-start_time}")

print(m_dict) 
    
