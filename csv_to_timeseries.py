import pandas as pd
import numpy as np
import os
import re 
import shutil
from pathlib import Path
import logging
from datetime import datetime
from time import time


import string
from sklearn.metrics.pairwise import cosine_similarity
from sklearn.feature_extraction.text import CountVectorizer
from nltk.corpus import stopwords
stopwords = stopwords.words('english')

pd.set_option('max_columns', 50)



def clean_string(text):

    text = text[:-4] 
    text = ''.join([word for word in text if word not in string.punctuation])
    text = text.lower()
    text = ' '.join([word for word in text.split() if word not in stopwords])
    return text

def cosine_sim_vectors(vec1,vec2):
    vec1 = vec1.reshape(1,-1)
    vec2 = vec2.reshape(1,-1)
    return cosine_similarity(vec1,vec2)[0][0]

def calculate_csim(statement_names):
    cleaned_statement_names = list(map(clean_string,statement_names))
    vectorizer = CountVectorizer().fit_transform(cleaned_statement_names)
    statement_name_vectors = vectorizer.toarray()
    csim = cosine_sim_vectors(statement_name_vectors[0],statement_name_vectors[1]) 
    return csim

def load_statements_into_dict(statement,csv_path,ticker,latest_ded,logging):

    df_dict = {}
    df_dict[latest_ded] = pd.read_csv(os.path.join(f"{csv_path}{ticker}/{latest_ded}",statement),index_col=[0])

    missing_list = []
    for dirname, _, filenames in os.walk(f"{csv_path}{ticker}"):
        missing_list.append(dirname)
        max_overlap = 0
        for filename in filenames:
            if (filename[-4:] == '.csv'): 

                csim = calculate_csim([statement, filename])
                if csim == 1:
                    most_likely_statement_match = filename 
                    max_overlap = 1
                    break #if filenames map perfectly don't even bother looking for column overlap
                elif csim > 0.3:
    
                    comp_df = pd.read_csv(os.path.join(dirname,filename),index_col=[0])
                    overlap_percentage = len(set(df_dict[latest_ded].index).intersection(comp_df.index))/len(df_dict[latest_ded])  
                
                    if overlap_percentage > max_overlap:
                        max_overlap = overlap_percentage
                        most_likely_statement_match = filename

        if max_overlap > 0.1: #making sure there's some non trivial overlap
            full_path = os.path.join(dirname,most_likely_statement_match)
            ds = re.search('[0-9]{4}-[0-9]{2}-[0-9]{2} \(10-[Q|K]\)',full_path)[0]
            df_dict[ds] = pd.read_csv(full_path,index_col=[0])
            missing_list.remove(dirname)

    missing_list = np.unique(missing_list)
    log_missing(missing_list,statement,csv_path, ticker,logging)

    return df_dict

def log_missing(missing_list,statement,csv_path,ticker,logging):
        
    missing_list = list(missing_list)
    missing_list.remove(f"{csv_path}{ticker}")

    for dirname in missing_list:
        warning_message = f"{statement} equivalent not found in {dirname}"
        print(warning_message) 
        logging.warning(warning_message) 

def populate_master_dataframe(date_statement_list,df_dict,logging):

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

    master_df = adjust_for_tag_changes_and_10k(master_df,date_statement_list,list_10k,df_dict,logging)


    return master_df


def adjust_for_10k(df_dict, list_10k, master_df,tag_map):
    list_10k = sorted(np.unique(np.concatenate(list_10k).flat),reverse=True)
    master_df = master_df.reindex(sorted(master_df.columns,reverse=True),axis=1)

    for item_10k in list_10k:
        df_dict_key = f"{item_10k} (10-K)"

        if df_dict_key in df_dict:
            tmp_df = df_dict[df_dict_key]

            tag_map_tmp_df_intersection = set(tag_map.keys()).intersection(tmp_df.index)
            if len(tag_map_tmp_df_intersection) > 0:

                for metric_match in tag_map_tmp_df_intersection: 
                    for metric in tag_map[metric_match]:
                        tmp_df.loc[metric,:] = tmp_df.loc[metric_match,:] 
                    tmp_df = tmp_df.drop(metric_match,axis=0)

            tmp_df = tmp_df[tmp_df['period_type'] == 'ytd'] 
            tmp_df = tmp_df[tmp_df.index.isin(master_df.index)]

            for date_col in [date_col for date_col in df_dict[df_dict_key].columns if re.match('[0-9]{4}-[0-9]{2}-[0-9]{2}',date_col)]: 
                if date_col in list_10k:
                    date_col_10k_index = (list(master_df.columns).index(date_col))

                    sequential_quarters = list(master_df.columns)[date_col_10k_index:(date_col_10k_index+4)]
                    sequential_quarters_count = count_sequential_quarters(sequential_quarters)
                    if sequential_quarters_count == 3:
                        not_nan_bool = master_df.loc[tmp_df.index,sequential_quarters].notna().all(axis=1)
                        not_nan_rows = tmp_df[not_nan_bool].index 
                        is_nan_bool = master_df.loc[tmp_df.index,sequential_quarters].isna().any(axis=1)
                        is_nan_rows = tmp_df[is_nan_bool].index 
                        master_df.loc[not_nan_rows,date_col] = tmp_df.loc[not_nan_rows,date_col] - master_df.loc[not_nan_rows,list(master_df.columns)[date_col_10k_index+1:date_col_10k_index+4]].sum(axis=1)  
                        master_df.loc[is_nan_rows,date_col] = None 
                        #master_df.loc[tmp_df.index,date_col] = tmp_df[date_col] - master_df.loc[tmp_df.index,list(master_df.columns)[date_col_10k_index+1:date_col_10k_index+4]].sum(axis=1) 
                    else:
                        master_df.loc[tmp_df.index,date_col] = None 

    return master_df

def adjust_for_tag_changes_and_10k(master_df,date_statement_list,list_10k,df_dict,logging): 

    tag_map = {} 
    
    tags_from_latest_statement = master_df[(master_df.isna().any(axis=1) & master_df[master_df.columns.max()].notna())]
    for metric in tags_from_latest_statement.index:
        for metric_match in master_df[~master_df.index.isin(tags_from_latest_statement.index)].index:

            non_nan_columns = master_df.loc[[metric,metric_match],:].notna().all(axis=0)
            agreement_series = master_df.loc[metric,non_nan_columns]==master_df.loc[metric_match,non_nan_columns]

            if (agreement_series.all() == True) and len(agreement_series > 1):
                    master_df.loc[metric,master_df.columns[master_df[master_df.index == metric].isna().iloc[0].to_list()]] = master_df.loc[metric_match,master_df.columns[master_df[master_df.index == metric].isna().iloc[0].to_list()]] 
                    if metric_match not in tag_map:
                        tag_map[metric_match] = [] 
                    tag_map[metric_match].append(metric) 

    for metric in np.unique(tag_map.keys()):
        master_df = master_df.drop(metric,axis=0)

    if len(list_10k) > 0:
        master_df = adjust_for_10k(df_dict, list_10k, master_df,tag_map)


    return master_df



def clean_up_dataframe(master_df,df_dict,date_statement_list,logging):
    latest_filing_mask = master_df.index.isin(df_dict[date_statement_list[0]].index)

    master_df_tmp = master_df.loc[latest_filing_mask,:]
    master_df_tmp = master_df_tmp.reindex(df_dict[date_statement_list[0]].index)
    master_df_tmp = master_df_tmp.append(master_df.loc[~latest_filing_mask,:])

    master_df_tmp = master_df_tmp.loc[~master_df_tmp.isna().all(axis=1),:]

    #master_df_tmp = 

    #master_df_tmp = master_df_tmp.reindex(date_statement_list,axis=1)


    return master_df_tmp

def pick_latest_statement(csv_path,ticker):
    ded_dict = {}
    for dirname in os.listdir(f"{csv_path}{ticker}"):
        ded_dict[dirname[:10]] = dirname 
    
    return ded_dict[max(ded_dict.keys())]

def count_sequential_quarters(quarter_list,log_missing=False,logging=None):
    sequential_quarters = 0
    for i in range(1,len(quarter_list)):
        diff = datetime.strptime(quarter_list[i-1],'%Y-%m-%d') - datetime.strptime(quarter_list[i],'%Y-%m-%d')
        if diff.days > 85 and diff.days < 95:
            sequential_quarters +=1
        else:
            if log_missing:
                message = f"Missing quarter between {quarter_list[i-1]} and {quarter_list[i]}"
                print_and_log(logging, message, True) 
            else:
                break   
    return sequential_quarters 

def check_dataframe(statement,master_df,date_statement_list,logging):


    expected_columns = round((datetime.strptime(master_df.columns.max(), "%Y-%m-%d")-datetime.strptime(master_df.columns.min(), "%Y-%m-%d")).days/(365/4))+1
    actual_columns = len(master_df.columns)
    if (expected_columns - actual_columns) >= 0:  
        message = f'Discontinuities estimate: {expected_columns-actual_columns}'

    sequential_quarters = count_sequential_quarters(master_df.columns,True,logging)
    message = f"Estimate of sequential quarters: {sequential_quarters}"
    print_and_log(logging,message)

    message = f"Number of periods: {actual_columns}" 
    print_and_log(logging,message)

    message = f"Number of metrics: {len(master_df)}" 
    print_and_log(logging,message)

    na_percentage = round(100*(len(master_df)-master_df.count()).sum()/(len(master_df)*len(master_df.columns)),2)
    message = f"NA percentage:  {na_percentage}"
    print_and_log(logging,message)

    master_df

    

def print_and_log(logging,message,warning=False):
    print(message)
    if warning:
        logging.warning(message)
    else:
        logging.info(message)


def add_labels_to_timeseries(master_df,df_dict,date_statement_list):

    for ds in date_statement_list:

        label_list = [l for l in df_dict[ds].columns if 'label' in l .lower()]

        for label in label_list:
            if label not in master_df.columns:
                master_df[label] = None

        if ds ==date_statement_list[0]:
            master_df.loc[ df_dict[ds].index,label_list] = df_dict[ds].loc[:,label_list]
        else:
            labels_to_find_index = df_dict[ds][df_dict[ds].index.isin(labels_to_find_list)].index
            master_df.loc[labels_to_find_index,label_list] = df_dict[ds].loc[labels_to_find_index,label_list] 

        label_list = [l for l in master_df.columns if 'label' in l .lower()] 

        if master_df[label_list].notna().any(axis=1).all() == False: #if any labels are still NA
            labels_to_find_list = master_df[master_df[label_list].notna().any(axis=1) == False].index
        else:
            break

    if 'label' not in master_df.columns:
        master_df['label'] = None

    for label in label_list:
        master_df.loc[master_df['label'].isna(),'label'] = master_df.loc[master_df['label'].isna(),label]  

    master_df['xbrl_tag'] = master_df.index 
    master_df = master_df.set_index(['label','xbrl_tag']) 
       
    for label in [l for l in master_df.columns if 'label' in l .lower()]: 
        master_df = master_df.drop(label,axis=1)
    
    return master_df


def save_file(master_df,timeseries_path, ticker,statement,logging):

    dash_list = [m.start() for m in re.finditer('-', statement)]
    if len(dash_list) >= 2: 
        filetype = statement[dash_list[0]+2:dash_list[1]-1]
        statement_name = statement[dash_list[1]+2:]
    else:
        message = f'Statement type could not be infered for {statement}'
        logging.warning(message) 
        print(message)
        statement_name = statement 
        filetype = 'Other'

    statement_folder = f"{timeseries_path}{ticker}/{filetype}/"
    Path(statement_folder).mkdir(parents=True, exist_ok=True)

    master_df.to_csv(f"{statement_folder}{statement_name}")
    

def populate_time_series(logging,latest_ded,statement,csv_path,ticker):

    print_and_log(logging,statement)

    df_dict = load_statements_into_dict(statement,csv_path,ticker,latest_ded,logging)
    date_statement_list = sorted(list(df_dict.keys()),reverse=True)

    master_df = populate_master_dataframe(date_statement_list,df_dict,logging) 
    master_df = clean_up_dataframe(master_df,df_dict,date_statement_list,logging)
    #master_df = add_labels_to_timeseries(master_df,df_dict,date_statement_list)
    check_dataframe(statement,master_df,date_statement_list,logging)

    return master_df


#### RUN CODE

csv_path = '../data/csv/'
log_path = '../data/logs/'

timeseries_path = '../data/timeseries/'

ticker_list = next(os.walk(csv_path))[1]
ticker_list = ['ZM'] 



for ticker in ticker_list:




    logfilename = f"{log_path}csv_to_timeseries_{datetime.now().strftime('%Y_%m_%d__%H_%M')}.log"
    logging.basicConfig(filename=logfilename, filemode='w', format='%(levelname)s - %(message)s',level=logging.INFO)

    print_and_log(logging,ticker)

    overall_start_time = time()

    shutil.rmtree(f"{timeseries_path}{ticker}", ignore_errors=True, onerror=None)  #remove if exists 
    Path(f"{timeseries_path}{ticker}").mkdir(parents=True, exist_ok=True)

    latest_ded = pick_latest_statement(csv_path,ticker)


    for dirname, _, filenames in os.walk(f"{csv_path}{ticker}/{latest_ded}"):
        for statement in filenames:
            master_df = populate_time_series(logging,latest_ded,statement, csv_path, ticker)
            save_file(master_df,timeseries_path, ticker,statement,logging)
        
    time_taken = f"Total: time: {time() - overall_start_time}"
    print_and_log(logging,time_taken)

