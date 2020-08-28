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

from utils import setup_logging,upload_statement_files, download_statement_files, delete_statement_files

import json

import requests
import urllib.parse

import sys


class CompanyStatementStandardize: 

    def __init__(self,ticker,data_path,overall_logger,bucket_name):

        overall_start_time = time()
        #initialize key variables
        self.ticker = ticker 
        self.log_path = f"{data_path}logs/{ticker}/{overall_logger.name[6:]}/"
        self.data_root_path = f"{data_path}data/{ticker}/"
        self.timeseries_root_path = f"{self.data_root_path}timeseries/"
        self.timeseries_statement_path = f"{self.timeseries_root_path}Raw Statements/"
        self.canonical_timeseries_statement_path = f"{self.timeseries_root_path}Clean Statements/"

        standardized_logger = setup_logging(self.log_path,'standardize_timeseries.log',f'standardized_{ticker}')
        standardized_logger.info(f'_____{ticker}_TIMESERIES_STANDARDIZED_____')


        shutil.rmtree(f"{self.canonical_timeseries_statement_path}", ignore_errors=True, onerror=None)  #remove if exists - this is fast so haven't bothered with adding update only functionality:w
        Path(self.canonical_timeseries_statement_path).mkdir(parents=True, exist_ok=True)

        with open(f"mapping/canonical_label_tag_mapping.json") as json_file:
            self.mapping_dict = json.load(json_file)

        statement_dict = {}
        for canonical_statement in ['Income Statement','Cash Flow','Balance Sheet']:
            statement_dict[canonical_statement] = self.identify_statement(canonical_statement,overall_logger,standardized_logger) 

            df_timeseries = pd.read_csv(f'{self.timeseries_statement_path}{statement_dict[canonical_statement]}',index_col=[0,1]) 
            df_timeseries = self.add_standard_label(df_timeseries,canonical_statement)
            df_timeseries = self.clean_up_report(df_timeseries,standardized_logger,overall_logger)
            df_timeseries.to_csv(f'{self.canonical_timeseries_statement_path}{ticker} {canonical_statement}.csv')


        upload_statement_files(bucket_name, data_path,"timeseries",ticker,standardized_logger)
        shutil.rmtree(f"{self.data_root_path}", ignore_errors=True, onerror=None)  #remove if exists 

        time_taken = f"Total time: {time() - overall_start_time}"
        standardized_logger.info(time_taken)


    def clean_up_report(self,df_timeseries,standardized_logger,overall_logger):

        #drop empty columns (do this first in case min data is empty)
        df_timeseries = df_timeseries.drop(df_timeseries.loc[:,df_timeseries.isna().all()].columns,axis=1)

        df_timeseries = self.drop_extraneous_columns(df_timeseries,standardized_logger) 
        df_timeseries = self.add_missing_columns(df_timeseries,standardized_logger,overall_logger) 
        df_timeseries = self.add_missing_rows(df_timeseries) 


        return df_timeseries 

    def drop_extraneous_columns(self,df_timeseries,standardized_logger): 

        date_col_list = df_timeseries.columns

        if self.uses_regular_quarter_schedule(df_timeseries):
            regular_quarter_dates = [date_col for date_col in date_col_list if date_col[-5:]in ['03-31','06-30','09-30','12-31']]
            drop_column_list = date_col_list[~pd.to_datetime(date_col_list).isin(pd.date_range(start=min(regular_quarter_dates),end=max(regular_quarter_dates),freq="3M"))]
        else:
            drop_column_list = []
            for i in range(1,len(date_col_list)):
                diff = datetime.strptime(date_col_list[i-1],'%Y-%m-%d') - datetime.strptime(date_col_list[i],'%Y-%m-%d')
                if diff.days < 85: 
                    drop_column_list.append(df_timeseries.loc[:,[date_col_list[i-1],date_col_list[i]]].isna().sum().idxmax())
        
            drop_column_list = list(np.unique(drop_column_list))

        standardized_logger.info(f"Dropped {', '.join(drop_column_list)}")
        df_timeseries = df_timeseries.drop(drop_column_list,axis=1)
        return df_timeseries 

    def add_missing_columns(self,df_timeseries,standardized_logger,overall_logger): 

        date_col_list = df_timeseries.columns

        if self.uses_regular_quarter_schedule(df_timeseries):
            regular_quarter_dates = [date_col for date_col in date_col_list if date_col[-5:] in ['03-31','06-30','09-30','12-31']]
            datetime_range = pd.date_range(start=min(regular_quarter_dates),end=max(regular_quarter_dates),freq="3M")
            missing_dates = datetime_range[~datetime_range.isin(pd.to_datetime(date_col_list))].astype(str)
        else:
            missing_dates = []
            for i in range(1,len(date_col_list)):
                diff = datetime.strptime(date_col_list[i-1],'%Y-%m-%d') - datetime.strptime(date_col_list[i],'%Y-%m-%d')
                if diff.days > 103: 
                    for j in range(1,int(round(diff.days/91,0))): #loop through all the missing dates
                        missing_dates.append(datetime.strftime(datetime.strptime(date_col_list[i],'%Y-%m-%d') + timedelta(days=91*j),'%Y-%m-%d'))

            df_timeseries = df_timeseries.merge(pd.DataFrame(index=df_timeseries.index,columns=missing_dates),left_index=True,right_index=True)
            df_timeseries = df_timeseries.reindex(sorted(df_timeseries.columns,reverse=True),axis=1)#else:
            overall_logger.error(f"Missing dates: {', '.join(missing_dates)}")
            standardized_logger.error(f"Missing dates: {', '.join(missing_dates)}")
    
        return df_timeseries

    def add_missing_rows(self,df_timeseries):

        #for now, just adding gross income
        if ('Gross income' not in df_timeseries.index.get_level_values(2)) and (df_timeseries.index.get_level_values(2).isin(['Revenue','Cost of revenue']).sum() == 2): 
            df_gross_income = pd.DataFrame(df_timeseries[df_timeseries.index.get_level_values(2) == 'Revenue'].values - df_timeseries[df_timeseries.index.get_level_values(2) == 'Cost of revenue'].values,index=pd.MultiIndex.from_tuples(list(zip([None],[None],['Gross income'])),names=['filing_label','xbrl_tag','standard_label']),columns=df_timeseries.columns)
            split_df_loc = df_timeseries.index.get_level_values(2).get_loc('Cost of revenue')+1
            df_timeseries= pd.concat([df_timeseries.iloc[:split_df_loc], df_gross_income, df_timeseries.iloc[split_df_loc:]]) 

        return df_timeseries


    def uses_regular_quarter_schedule(self,df_timeseries):
        quarter_list = df_timeseries.columns
        regular_quarter_dates = [date_col for date_col in quarter_list if date_col[-5:] in ['03-31','06-30','09-30','12-31']]
        if len(regular_quarter_dates)/len(quarter_list) > 0.7: #if 70% of data columns are part of regular observation then 
            return True
        else:
            return False


    def identify_statement(self,canonical_statement,overall_logger,standardized_logger):

        tag_list = self.flatten([self.mapping_dict[canonical_statement][label_list] for label_list in self.mapping_dict[canonical_statement]])

        max_overlap = 0
        for statement_csv in os.listdir(self.timeseries_statement_path):
            if statement_csv[-4:] == '.csv':
                df_timeseries = pd.read_csv(f'{self.timeseries_statement_path}{statement_csv}',index_col=[0,1])
                df_timeseries = df_timeseries.loc[:,max(df_timeseries.columns)].dropna() #only want to match against most recent month
                match_count = df_timeseries.index.get_level_values(1).isin(tag_list).sum()
                if match_count > max_overlap:
                    matched_statement = statement_csv                        
                    max_overlap = match_count

        overall_logger.info(f"{canonical_statement} matched with {matched_statement} ({max_overlap} matches)")
        standardized_logger.info(f"{canonical_statement} matched with {matched_statement} ({max_overlap} matches)")

        return matched_statement


    def add_standard_label(self,df_timeseries,canonical_statement):
        df_timeseries_tmp = df_timeseries
        df_timeseries_tmp = df_timeseries_tmp.loc[:,[max(df_timeseries_tmp.columns)]].dropna() #only want to match against most recent month
        df_timeseries_tmp['standard_label'] = None

        for standard_label in self.mapping_dict[canonical_statement]:
            for tag in self.mapping_dict[canonical_statement][standard_label]:
                if tag in df_timeseries_tmp.index.get_level_values(1).str.extract(r'([A-Za-z-_]+)___')[0].tolist():#checking for segment                  
                    segment_slice = pd.Series(list(tag == df_timeseries_tmp.index.get_level_values(1).str.extract(r'([-A-Za-z_]+)___')[0]),index=df_timeseries_tmp.index)
                    if df_timeseries_tmp.loc[segment_slice,:].index.get_level_values(0).notna().all(): #making sure labels aren't null
                        df_timeseries_tmp.loc[segment_slice,'standard_label'] = pd.Series(list(standard_label + ' ' + df_timeseries_tmp.loc[segment_slice,:].index.get_level_values(0).str.extract(r'(\([ A-Za-z\[\]]+\))')[0]),index=df_timeseries_tmp.loc[segment_slice,:].index)

                if tag in df_timeseries_tmp.index.get_level_values(1):
                    label = df_timeseries_tmp[df_timeseries_tmp.index.get_level_values(1) == tag].index.get_level_values(0)[0]
                    if label is not None:
                        df_timeseries_tmp.loc[(label,tag), 'standard_label'] = standard_label 
                        break #only want to add the first match
                
        return self.merge_new_label_and_correct_index(df_timeseries,df_timeseries_tmp['standard_label'])

    def merge_new_label_and_correct_index(self,df_timeseries,s_standard_label):
        df_timeseries = df_timeseries.merge(s_standard_label,left_index=True,right_index=True,how="left")
        df_timeseries['filing_label'] = df_timeseries.index.get_level_values(0)
        df_timeseries['xbrl_tag'] = df_timeseries.index.get_level_values(1)
        df_timeseries = df_timeseries.set_index(['filing_label','xbrl_tag','standard_label'])
        return df_timeseries

    def flatten(self,xs):
        res = []
        def loop(ys):
            for i in ys:
                if isinstance(i, list):
                    loop(i)
                else:
                    res.append(i)
        loop(xs)
        return res


  

