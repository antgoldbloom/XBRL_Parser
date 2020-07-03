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

def load_statements_into_dict(statement,csv_path,ticker,latest_ded,logging):

    df_dict = {}


    missing_list = []
    for dirname, _, filenames in os.walk(f"{csv_path}{ticker}"):
        for filename in filenames:
            if (filename[-4:] == '.csv'): 
                missing_list.append(dirname)
                if statement.lower() == filename.lower(): 
                    full_path = os.path.join(dirname,filename)
                    df_dict[full_path[17:34]] = pd.read_csv(full_path,index_col=[0])
                    missing_list.remove(dirname)

    missing_list = np.unique(missing_list)

    df_dict = fill_gaps_and_log_missing(df_dict,missing_list,logging,statement,latest_ded)
                
    return df_dict


def fill_gaps_and_log_missing(df_dict,missing_list,logging,statement,latest_ded):

    reference_df = df_dict[latest_ded]

    additional_found_statements = []
    
    for dirname in missing_list:
        max_overlap = 0
        for filename in os.listdir(dirname):
            comp_df = pd.read_csv(os.path.join(dirname,filename),index_col=[0])
            overlap_percentage = len(set(reference_df.index).intersection(comp_df.index))/len(reference_df) 

            if overlap_percentage > max_overlap:
                max_overlap = overlap_percentage
                most_likely_statement_match = filename 

        if max_overlap > 0.6: #might need to tune this number
            full_path = os.path.join(dirname,most_likely_statement_match)
            df_dict[full_path[17:34]] = pd.read_csv(full_path,index_col=[0])

            additional_found_statements.append(dirname) 

    additional_found_statements = np.unique(additional_found_statements)
    missing_list = list(missing_list)
    for dirname in additional_found_statements:
        missing_list.remove(dirname)
    
    #log_missing(missing_list,statement,logging)

    return df_dict 

def log_missing(missing_list,statement,logging):
        
    for dirname in missing_list:
        warning_message = f"no {statement} equivalent found in {dirname}"
        print(warning_message) 
        logging.warning(warning_message) 




def populate_time_series(logging,latest_ded):

    df_dict = load_statements_into_dict(statement,csv_path,ticker,latest_ded,logging)
    date_statement_list = sorted(list(df_dict.keys()),reverse=True)

    list_10k = []

    for ds in date_statement_list:
        
        date_cols = [date_col for date_col in df_dict[ds].columns if re.match('[0-9]{4}-[0-9]{2}-[0-9]{2}',date_col)]
        tmp_df = df_dict[ds][date_cols]

        if ds[12:16] == '10-K':
            list_10k.append(date_cols)

        if ds == date_statement_list[0]:
            master_df = tmp_df
        else:
            new_columns = tmp_df.columns.difference(master_df.columns)
            master_df = master_df.merge(tmp_df[new_columns],left_index=True,right_index=True,how='outer')
            master_df.loc[master_df.index.isin(tmp_df.index),master_df.columns.isin(tmp_df)] = tmp_df

    master_df = adjust_for_tag_changes(master_df,date_statement_list,logging)
    
    if len(list_10k) > 0:
        master_df = adjust_for_10k(df_dict, list_10k, master_df)

    master_df = reorder_dataframe(master_df,df_dict,date_statement_list,logging)
    master_df = add_labels_to_timeseries(master_df,df_dict,date_statement_list)

    check_dataframe(statement,master_df,date_statement_list,logging)

    return master_df

def adjust_for_10k(df_dict, list_10k, master_df):
    for item_10k in list_10k:
        df_dict_key = f"{item_10k} (10-K)"

        if df_dict_key in df_dict:
            tmp_df = df_dict[df_dict_key]
            tmp_df = tmp_df[tmp_df['period_type'] == 'ytd'] 
            tmp_df = tmp_df[tmp_df.index.isin(master_df.index)]
            item_10k_index = list(master_df.columns).index(item_10k)
        
            master_df.loc[tmp_df.index,item_10k] = master_df.loc[tmp_df.index,item_10k] - master_df.loc[tmp_df.index,list(master_df.columns)[item_10k_index+1:item_10k_index+4]].sum(axis=1) 

    list_10k = sorted(np.unique(np.concatenate(list_10k).flat),reverse=True)
    master_df = master_df.reindex(sorted(master_df.columns,reverse=True),axis=1)

    return master_df

def adjust_for_tag_changes(master_df,date_statement_list,logging): 


    drop_list = []    
    tags_from_latest_statement = master_df[(master_df.isna().any(axis=1) & master_df[date_statement_list[0][0:10]].notna())]
    for metric in tags_from_latest_statement.index:
        for metric_match in master_df[~master_df.index.isin(tags_from_latest_statement.index)].index:

            non_nan_columns = master_df.loc[[metric,metric_match],:].notna().all(axis=0)
            agreement_series = master_df.loc[metric,non_nan_columns]==master_df.loc[metric_match,non_nan_columns]

            if (agreement_series.all() == True) and len(agreement_series > 1):
                    master_df.loc[metric,master_df.columns[master_df[master_df.index == metric].isna().iloc[0].to_list()]] = master_df.loc[metric_match,master_df.columns[master_df[master_df.index == metric].isna().iloc[0].to_list()]] 
                    drop_list.append(metric_match)

    for metric in np.unique(drop_list):
        master_df = master_df.drop(metric,axis=0)



    return master_df

#def adjust_by_matching_labels(master_df,df_dict):




def reorder_dataframe(master_df,df_dict,date_statement_list,logging):
    latest_filing_mask = master_df.index.isin(df_dict[date_statement_list[0]].index)

    master_df_tmp = master_df.loc[latest_filing_mask,:]
    master_df_tmp = master_df_tmp.reindex(df_dict[date_statement_list[0]].index)
    master_df_tmp = master_df_tmp.append(master_df.loc[~latest_filing_mask,:])

    #master_df_tmp = master_df_tmp.reindex(date_list,axis=1)


    return master_df_tmp

def pick_latest_statement(csv_path,ticker):
    ded_dict = {}
    for dirname in os.listdir(f"{csv_path}{ticker}"):
        ded_dict[dirname[:10]] = dirname 
    
    return ded_dict[max(ded_dict.keys())]

def check_dataframe(statement,master_df,date_statement_list,logging):

    print(statement)
    logging.info(statement)

    expected_columns = round((datetime.strptime(master_df.columns.max(), "%Y-%m-%d")-datetime.strptime(master_df.columns.min(), "%Y-%m-%d")).days/(365/4))+1
    actual_columns = len(master_df.columns)
    if (expected_columns - actual_columns) >= 0:  
        message = f'Discontinuities estimate: {expected_columns-actual_columns}'
        logging.info(message)
        print(message)

    na_percentage = round(100*(len(master_df)-master_df.count()).sum()/(len(master_df)*len(master_df.columns)),2)
    message = f"NA percentage:  {na_percentage}"
    print(message)
    logging.info(message)

    message = f"Number of periods: {actual_columns}" 
    print(message)
    logging.info(message)

    message = f"Number of metrics: {len(master_df)}" 
    print(message)
    logging.info(message)

def which_label_types_in_dataframe(df):
    label_list = []
    label_types = ['label','terseLabel','verboseLabel']
    for label_type in label_types: 
        if label_type in df.columns:
            label_list.append(label_type)
    return label_list

def add_labels_to_timeseries(master_df,df_dict,date_statement_list):

    for ds in date_statement_list:

        label_list = which_label_types_in_dataframe(df_dict[ds]) 

        if ds ==date_statement_list[0]:
            for label in ['label','verboseLabel','terseLabel']: 
                master_df[label] = None
            master_df.loc[ df_dict[ds].index,label_list] = df_dict[ds].loc[:,label_list]
        else:
            labels_to_find_index = df_dict[ds][df_dict[ds].index.isin(labels_to_find_list)].index
            master_df.loc[labels_to_find_index,label_list] = df_dict[ds].loc[labels_to_find_index,label_list] 

        label_list = which_label_types_in_dataframe(master_df)

        if master_df[label_list].notna().any(axis=1).all() == False: #if any labels are still NA
            labels_to_find_list = master_df[master_df[label_list].notna().any(axis=1) == False].index
        else:
            break

            
    master_df.loc[master_df['label'].isna(),'label'] = master_df.loc[master_df['label'].isna(),'terseLabel']  
    master_df.loc[master_df['label'].isna(),'label'] = master_df.loc[master_df['label'].isna(),'verboseLabel']  
    master_df['xbrl_tag'] = master_df.index 
    master_df = master_df.set_index(['label','xbrl_tag']) 
       
    for label in which_label_types_in_dataframe(master_df):
        master_df = master_df.drop(label,axis=1)
    
    return master_df

    
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
        

        master_df = populate_time_series(logging,latest_ded)

        master_df.to_csv(f"{timeseries_path}{ticker}/{statement}")
        
print(f"Total: time: {time() - overall_start_time}")
logging.info(f"Total: time: {time() - overall_start_time}")
