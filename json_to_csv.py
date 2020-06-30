import json
import numpy as np
import pandas as pd
import os
from pathlib import Path
import re
import glob
from time import time

from datetime import datetime
import shutil
import logging

json_path = '../data/json/'
csv_path = '../data/csv/'
log_path = '../data/logs/'

#statement_name = 'SegmentInformationReportableSegmentsAndReconciliationToConsolidatedNetIncomeDetails'
#statement_name = 'SegmentInformationDisaggregationOfRevenueDetails'
 
def load_json_to_dict(json_path,ticker):

    with open(f"{json_path}{ticker}.json", 'r') as stock_json:
        stock_dict = json.loads(stock_json.read())

    return stock_dict

def list_top_level_statement_tags(stock_dict_with_ded,statement):
    top_level_xlink_from = [] 
    #idetify top metric_label
    for metric in stock_dict_with_ded[statement]['metrics']:
        metric_dict_slice = stock_dict_with_ded[statement]['metrics'][metric] 
        if ('prearc_xlink:from' not in metric_dict_slice): 
            top_level_xlink_from.append(stock_dict_with_ded[statement]['metrics'][metric]['prearc_xlink:to'])
    return top_level_xlink_from

def create_tmp_stock_dict(stock_dict_with_ded_statement_metric,metric):
    if metric == 'us-gaap_revenuefromcontractwithcustomerexcludingassessedtax':
        print(metric)
    dict_key_list = ['label','prearc_order', 'prearc_xlink:from','prearc_xlink:to','qtd','ytd','instant']
    tmp_stock_dict = dict()
    tmp_stock_dict['metric'] = metric 
    for key in dict_key_list:
        if key in stock_dict_with_ded_statement_metric: 
            tmp_stock_dict[key] = stock_dict_with_ded_statement_metric[key]
    return tmp_stock_dict


def add_to_stock_list_dict(stock_dict_with_ded,stock_list_dict,statement,metric,stock_dict_up_to_metrics,logging,is_segment=False,sld_index=None):
    tmp_stock_dict_list = []
    top_level_xlink_from = list_top_level_statement_tags(stock_dict_with_ded,statement) 
    tmp_stock_dict = create_tmp_stock_dict(stock_dict_up_to_metrics[metric],metric)
    tmp_stock_dict_list.append(tmp_stock_dict)

    if not is_segment:
        stock_list_dict = walk_prearc_xlink_tree(top_level_xlink_from,metric,stock_dict_up_to_metrics, stock_list_dict,tmp_stock_dict_list,logging)  
    else: # this is for segments
        stock_list_dict[sld_index] = tmp_stock_dict_list 

    return stock_list_dict


def walk_prearc_xlink_tree(top_level_xlink_from,metric,stock_dict_up_to_metrics, stock_list_dict,tmp_stock_dict_list,logging): 
    if 'prearc_xlink:from' in stock_dict_up_to_metrics[metric]:
        xlink_lower_metric = stock_dict_up_to_metrics[metric]['prearc_xlink:from']
        xlink_lower_metric_match = None 

        WHILE_COUNT_CUTOFF = 10
        while_count = 0 
        while ( (xlink_lower_metric_match not in top_level_xlink_from)):
            for metric_lower in stock_dict_up_to_metrics:  
                xlink_lower_metric_match = stock_dict_up_to_metrics[metric_lower]['prearc_xlink:to'] 
                if (xlink_lower_metric_match == xlink_lower_metric) and (xlink_lower_metric_match not in top_level_xlink_from):

                    tmp_stock_dict = create_tmp_stock_dict(stock_dict_up_to_metrics[metric_lower],metric_lower)
                    tmp_stock_dict_list.append(tmp_stock_dict)

                    xlink_lower_metric = stock_dict_up_to_metrics[metric_lower]['prearc_xlink:from']

                elif (xlink_lower_metric_match == xlink_lower_metric) and (xlink_lower_metric_match in top_level_xlink_from):
                    
                    tmp_stock_dict = create_tmp_stock_dict(stock_dict_up_to_metrics[metric_lower],metric_lower)
                    tmp_stock_dict_list.append(tmp_stock_dict)

                    stock_list_dict[metric] = tmp_stock_dict_list 
                    break

            
            if (xlink_lower_metric_match not in top_level_xlink_from) and (while_count == WHILE_COUNT_CUTOFF): 
                logging.warning(f"Skipped {metric_lower} due to error in presentation file") 
                break #GIVE UP ON FINDING MATCHES AT THIS POINT

            while_count+=1

    return stock_list_dict 
        



def create_stock_dict_list(stock_dict_with_ded,statement,freq,logging):
    stock_list_dict = dict()
    #pull out label chain for each numeric metric into dict
    for metric in stock_dict_with_ded[statement]['metrics']:
        if (freq in stock_dict_with_ded[statement]['metrics'][metric]) or ('instant' in stock_dict_with_ded[statement]['metrics'][metric]): 
            stock_list_dict = add_to_stock_list_dict(stock_dict_with_ded,stock_list_dict,statement,metric,stock_dict_with_ded[statement]['metrics'],logging)
        if 'segment' in stock_dict_with_ded[statement]['metrics'][metric]: 
            for segment_metric in stock_dict_with_ded[statement]['metrics'][metric]['segment']:
                if (freq in stock_dict_with_ded[statement]['metrics'][metric]['segment'][segment_metric]) or ('instant' in stock_dict_with_ded[statement]['metrics'][metric]['segment'][segment_metric]):   
                    sld_index = f"{metric}___{segment_metric}"
                    stock_list_dict = add_to_stock_list_dict(stock_dict_with_ded,stock_list_dict,statement,segment_metric,stock_dict_with_ded[statement]['metrics'][metric]['segment'],logging,True,sld_index) #order segments just below parent item
                    if metric in stock_list_dict:
                        stock_list_dict[sld_index][0]['prearc_order'] = float(stock_list_dict[metric][0]['prearc_order']) + 0.1
                        for i in range(1,len(stock_list_dict[sld_index])):
                            stock_list_dict[sld_index][i]['prearc_order'] = stock_list_dict[metric][i]['prearc_order'] 
    return stock_list_dict                    


def stock_list_dict_to_dataframe(stock_dict_with_ded,document_end_date,document_type,statement,freq,logging):

    stock_list_dict = create_stock_dict_list(stock_dict_with_ded,statement,freq,logging) 

    metric_list = []
    for row in stock_list_dict:
        metric_list.append(row)

    df_statement = pd.DataFrame(index=metric_list)  


    for metric in stock_list_dict:
        if 'label' in stock_list_dict[metric][0]:
            df_statement.loc[df_statement.index==metric,'label'] =  stock_list_dict[metric][0]['label']   
        else: 
            print('here')

        for period_type in [freq,'instant']: 
            if period_type in stock_list_dict[metric][0]:
                for metric_date in stock_list_dict[metric][0][period_type]:
                    df_statement.loc[df_statement.index==metric,metric_date] =  stock_list_dict[metric][0][period_type][metric_date]
        
    df_statement = df_statement.iloc[:, ::-1] #reverse order of dates

    if len(df_statement) > 0:
        save_statement(df_statement,stock_dict, document_end_date,document_type,statement)
    else: 
        logging.warning(f"{statement} for {document_end_date} was not saved")


def make_filename_safe(statement_name):
    keepcharacters = (' ','-','_')
    statement_name = "".join(c for c in statement_name if c.isalnum() or c in keepcharacters).rstrip()

    if len(statement_name) > 250:
        statement_name = statement_name[:250]

    return statement_name


def save_statement(df_statement,stock_dict, document_end_date,document_type,statement):
    statement_name = stock_dict[document_end_date]['statements'][statement]['statement_name']

    dash_list = [m.start() for m in re.finditer('-', statement_name)]
    if len(dash_list) >= 2: 
        filetype = statement_name[dash_list[0]+2:dash_list[1]-1]
    else:
        filetype = 'Other'

    statement_folder = f"{csv_path}{ticker}/{document_end_date} ({document_type})/{filetype}/"
    
    Path(statement_folder).mkdir(parents=True, exist_ok=True)
    
    statement_name = make_filename_safe(statement_name) 

    df_statement.to_csv(f"{statement_folder}{statement_name}.csv")



def extract_freq(stock_dict,document_end_date):
    if stock_dict[document_end_date]['document_type'] == '10-Q':  
        freq = 'qtd'
    elif stock_dict[document_end_date]['document_type'] == '10-K':  
        freq = 'ytd'

    return freq

file_list = glob.glob(f"{json_path}/*.json") 
file_list = [f"{json_path}AMZN.json"]


logfilename = f"{log_path}json_to_csv_{datetime.now().strftime('%Y_%m_%d__%H_%M')}.log"
logging.basicConfig(filename=logfilename, filemode='w', format='%(levelname)s - %(message)s',level=logging.INFO)

overall_start_time = time()

for file_str in file_list: 

    
    ticker = file_str[len(json_path):-5] 
    shutil.rmtree(f"{csv_path}{ticker}", ignore_errors=True, onerror=None)  #remove if exists 

    print(ticker)
    start_time = time()
    logging.info(ticker)
    stock_dict= load_json_to_dict(json_path,ticker)


    for document_end_date in stock_dict: 

        freq = extract_freq(stock_dict,document_end_date)

        for statement in stock_dict[document_end_date]['statements']: 

            #if statement == 'consolidatedstatementsofoperations':
            #    print(statement)

            stock_list_dict_to_dataframe(stock_dict[document_end_date]['statements'],document_end_date,stock_dict[document_end_date]['document_type'],statement,freq,logging)
 
    ticker_time = f"{time() - start_time}"
    logging.info(ticker_time)
    print(ticker_time)

print(f"Total: time: {time() - overall_start_time}")
logging.info(f"Total: time: {time() - overall_start_time}")

    ####THE LIST IS CURRENT CORRECT ORDERED BY DEFAULT. CASN USE THIS IF THAT CHANGES
    #reorder_list = []
    #LEARN defaultdict in Python. That looks more flexible 
    #for metric in stock_list_dict:
    #    order_tmp = []
    #    for i in range(len(stock_list_dict[metric])-2,-1,-1):
    #        order_tmp.append(int(stock_list_dict[metric][i]['prearc_order'])) 
    #    reorder_list.append(order_tmp)

    #TO DO MAP REORDERING
    #print(sorted(reorder_list,reverse=False))

