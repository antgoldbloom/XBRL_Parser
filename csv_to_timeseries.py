import pandas as pd
import numpy as np
import os
import re 
import shutil
from pathlib import Path
import logging
from datetime import datetime
from time import time

pd.set_option('max_columns', 50)

def load_statements_into_dict(statement,csv_path,ticker,logging):

    df_dict = {}

    log_list = []
    for dirname, _, filenames in os.walk(f"{csv_path}{ticker}"):
        if '(10-Q)' in dirname:
            log_list.append(dirname)
            for filename in filenames:
                if statement[statement.lower().find('statement'):].lower() == filename[filename.lower().find('statement'):].lower(): 
                    full_path = os.path.join(dirname,filename)
                    df_dict[full_path[17:27]] = pd.read_csv(full_path,index_col=[0])
                    log_list.remove(dirname)

    for dirname in log_list:
        logging.warning(f'{statement} equivalent not found in {dirname}')
                
    return df_dict


def populate_time_series(df_dict,date_list,logging):


    for date in date_list:
        
        date_cols = [date_col for date_col in df_dict[date].columns if re.match('[0-9]{4}-[0-9]{2}-[0-9]{2}',date_col)]
        tmp_df = df_dict[date][date_cols]

        if date == date_list[0]:
            master_df = tmp_df
        else:
            #df_dict.
            new_columns = tmp_df.columns.difference(master_df.columns)
            master_df = master_df.merge(tmp_df[new_columns],left_index=True,right_index=True,how='outer')
            master_df.loc[master_df.index.isin(tmp_df.index),master_df.columns.isin(tmp_df)] = tmp_df

    master_df.index.name = df_dict[date_list[0]].index.name #Preserve index name (it drops if you merge tables with different index names) 

    return master_df

def adjust_for_tag_changes(master_df,date_list,logging): 
    drop_list = []    
    for metric in master_df[(master_df.isna().any(axis=1) & master_df[date_list[0]].notna())].index:
        for metric_match in master_df[master_df.index != metric].index:

            non_nan_columns = master_df.loc[[metric,metric_match],:].notna().all(axis=0)
            agreement_series = master_df.loc[metric,non_nan_columns]==master_df.loc[metric_match,non_nan_columns]

            if (agreement_series.all() == True) and len(agreement_series > 1):
                    master_df.loc[metric,master_df.columns[master_df[master_df.index == metric].isna().iloc[0].to_list()]] = master_df.loc[metric_match,master_df.columns[master_df[master_df.index == metric].isna().iloc[0].to_list()]] 
                    drop_list.append(metric_match)

    for metric in drop_list:
        master_df = master_df.drop(metric,axis=0)

    return master_df

#def adjust_by_matching_labels(master_df,df_dict):




def reorder_dataframe(master_df,date_list,logging):
    latest_filing_mask = master_df.index.isin(df_dict[date_list[0]].index)

    master_df_tmp = master_df.loc[latest_filing_mask,:]
    master_df_tmp = master_df_tmp.reindex(df_dict[date_list[0]].index)
    master_df_tmp = master_df_tmp.append(master_df.loc[~latest_filing_mask,:])

    master_df_tmp = master_df_tmp.reindex(date_list,axis=1)


    return master_df_tmp

def pick_latest_statement(csv_path,ticker):
    ded_dict = {}
    for dirname in os.listdir(f"{csv_path}{ticker}"):
        ded_dict[dirname[:10]] = dirname 
    
    return ded_dict[max(ded_dict.keys())]


csv_path = '../data/csv/'
log_path = '../data/logs/'

timeseries_path = '../data/timeseries/'
ticker = 'AMZN'

logfilename = f"{log_path}csv_to_timeseries_{datetime.now().strftime('%Y_%m_%d__%H_%M')}.log"
logging.basicConfig(filename=logfilename, filemode='w', format='%(levelname)s - %(message)s',level=logging.INFO)

overall_start_time = time()

shutil.rmtree(f"{timeseries_path}{ticker}", ignore_errors=True, onerror=None)  #remove if exists 
Path(f"{timeseries_path}{ticker}").mkdir(parents=True, exist_ok=True)

latest_ded = pick_latest_statement(csv_path,ticker)


for dirname, _, filenames in os.walk(f"{csv_path}{ticker}/{latest_ded}"):
    for statement in filenames:
        
        if 'Statement' in statement:
            df_dict = load_statements_into_dict(statement,csv_path,ticker,logging)
            date_list = sorted(list(df_dict.keys()),reverse=True)
            master_df = populate_time_series(df_dict,date_list,logging)
            master_df = adjust_for_tag_changes(master_df,date_list,logging)
            master_df = reorder_dataframe(master_df,date_list,logging)

            master_df.to_csv(f"{timeseries_path}{ticker}/{statement}")
        
print(f"Total: time: {time() - overall_start_time}")
logging.info(f"Total: time: {time() - overall_start_time}")
