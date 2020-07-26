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


class CompanyStatementStandardize: 

    def __init__(self,ticker,data_path,overall_logger):

        overall_start_time = time()
        #initialize key variables
        self.ticker = ticker 
        self.log_path = f"{data_path}logs/{ticker}/{overall_logger.name[6:]}/"
        self.timeseries_statement_path = f"{data_path}timeseries/{ticker}/Statement/"
        self.canonical_timeseries_statement_path = f"{data_path}timeseries/{ticker}/Canonical Statement/"
        self.mapping_path = f"{data_path}mappings/"

        standardized_logger = setup_logging(self.log_path,'.log',f'standardized_{ticker}')
        standardized_logger.info(f'_____{ticker}_TIMESERIES_STANDARDIZED_____')


        shutil.rmtree(f"{self.canonical_timeseries_statement_path}", ignore_errors=True, onerror=None)  #remove if exists <<
        Path(self.canonical_timeseries_statement_path).mkdir(parents=True, exist_ok=True)

        with open(f"{self.mapping_path}/canonical_label_tag_mapping.json") as json_file:
            self.mapping_dict = json.load(json_file)

        statement_dict = {}
        for canonical_statement in ['Income Statement','Cash Flow','Balance Sheet']:
            statement_dict[canonical_statement] = self.identify_statement(canonical_statement,standardized_logger) 

            df_timeseries = pd.read_csv(f'{self.timeseries_statement_path}{statement_dict[canonical_statement]}',index_col=[0,1]) 
            df_timeseries = self.add_standard_label(df_timeseries)
            df_timeseries.to_csv(f'{self.canonical_timeseries_statement_path}{canonical_statement}.csv')


        time_taken = f"Total time: {time() - overall_start_time}"
        standardized_logger.info(time_taken)


    def identify_statement(self,canonical_statement,standardized_logger):

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

        standardized_logger.info(f"{canonical_statement} matched with {matched_statement} ({max_overlap} matches)")

        return matched_statement


    def add_standard_label(self,df_timeseries):
        df_timeseries_tmp = df_timeseries
        df_timeseries_tmp = df_timeseries_tmp.loc[:,[max(df_timeseries_tmp.columns)]].dropna() #only want to match against most recent month
        df_timeseries_tmp['standard_label'] = None

        for standard_label in self.mapping_dict['Income Statement']:
            for tag in self.mapping_dict['Income Statement'][standard_label]:
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


  

