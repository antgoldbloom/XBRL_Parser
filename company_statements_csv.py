import json
import numpy as np
import pandas as pd
import os
from pathlib import Path
import re
import glob
from time import time


from utils import setup_logging 


from datetime import datetime
import shutil


class CompanyStatementCSV:

    def __init__(self,ticker,data_path,overall_logger,update_only=True):


        ### initialize variables
        self.ticker = ticker 
        self.json_path = f'{data_path}json/'
        self.csv_path = f'{data_path}csv/{ticker}/'
        self.log_path = f'{data_path}logs/{ticker}/{overall_logger.name[6:]}/'

        #if not update only clear csv path
        if update_only == False:
            shutil.rmtree(f"{self.csv_path}", ignore_errors=True, onerror=None)  #remove if exists 

        #initialize logging
        csv_logger = setup_logging(self.log_path,'csv.log',f'csv_{ticker}')
        csv_logger.info(f'______{ticker}_CSV______')

        start_time = time()
        self.stock_dict= self.load_json_to_dict()

        #convert json to dataframe
        for document_end_date in self.stock_dict: 
            csv_logger.info(f"__{document_end_date}__")
            csv_full_path = f"{self.csv_path}{document_end_date} ({self.stock_dict[document_end_date]['document_type']})"
            if os.path.exists(csv_full_path):
                csv_logger.info(f"{document_end_date} ({self.stock_dict[document_end_date]['document_type']}) already exists so not updating")
            else:
                freq = self.extract_freq(document_end_date)
                for statement in self.stock_dict[document_end_date]['statements']: 
                    self.stock_list_dict_to_dataframe(self.stock_dict[document_end_date]['statements'],document_end_date,self.stock_dict[document_end_date]['document_type'],statement,freq,csv_logger)
 
        self.date_count = len(os.listdir(self.csv_path)) 
        csv_logger.info(f"Statement Count: {self.date_count}")
        ticker_time = f"{time() - start_time}"
        csv_logger.info(f"Total time: {ticker_time}")


 
    def load_json_to_dict(self):

        with open(f"{self.json_path}{self.ticker}.json", 'r') as stock_json:
            stock_dict = json.loads(stock_json.read())


        return stock_dict

    def list_top_level_statement_tags(self,stock_dict_with_ded,statement):
        top_level_xlink_from = [] 
        #idetify top metric_label
        for metric in stock_dict_with_ded[statement]['metrics']:
            metric_dict_slice = stock_dict_with_ded[statement]['metrics'][metric] 
            if ('prearc_xlink:from' not in metric_dict_slice): 
                top_level_xlink_from.append(stock_dict_with_ded[statement]['metrics'][metric]['prearc_xlink:to'])
        return top_level_xlink_from

    def create_tmp_stock_dict(self,stock_dict_with_ded_statement_metric,metric):
        dict_key_list = ['labels','prearc_order', 'prearc_xlink:from','prearc_xlink:to','qtd','ytd','instant']
        tmp_stock_dict = dict()
        tmp_stock_dict['metric'] = metric 
        for key in dict_key_list:
            if key in stock_dict_with_ded_statement_metric: 
                tmp_stock_dict[key] = stock_dict_with_ded_statement_metric[key]
        return tmp_stock_dict


    def add_to_stock_list_dict(self,stock_dict_with_ded,stock_list_dict,statement,metric,stock_dict_up_to_metrics,csv_logger,is_segment=False,sld_index=None):
        tmp_stock_dict_list = []
        top_level_xlink_from = self.list_top_level_statement_tags(stock_dict_with_ded,statement) 
        tmp_stock_dict = self.create_tmp_stock_dict(stock_dict_up_to_metrics[metric],metric)
        tmp_stock_dict_list.append(tmp_stock_dict)

        if not is_segment:
            stock_list_dict = self.walk_prearc_xlink_tree(top_level_xlink_from,metric,stock_dict_up_to_metrics, stock_list_dict,tmp_stock_dict_list,csv_logger)  
        else: # this is for segments
            stock_list_dict[sld_index] = tmp_stock_dict_list 

        return stock_list_dict


    def walk_prearc_xlink_tree(self,top_level_xlink_from,metric,stock_dict_up_to_metrics, stock_list_dict,tmp_stock_dict_list,csv_logger): 
        if 'prearc_xlink:from' in stock_dict_up_to_metrics[metric]:
            xlink_lower_metric = stock_dict_up_to_metrics[metric]['prearc_xlink:from']
            xlink_lower_metric_match = None 

            WHILE_COUNT_CUTOFF = 10
            while_count = 0 
            while ( (xlink_lower_metric_match not in top_level_xlink_from)):
                for metric_lower in stock_dict_up_to_metrics:  
                    xlink_lower_metric_match = stock_dict_up_to_metrics[metric_lower]['prearc_xlink:to'] 
                    if (xlink_lower_metric_match == xlink_lower_metric) and (xlink_lower_metric_match not in top_level_xlink_from):

                        tmp_stock_dict = self.create_tmp_stock_dict(stock_dict_up_to_metrics[metric_lower],metric_lower)
                        tmp_stock_dict_list.append(tmp_stock_dict)

                        xlink_lower_metric = stock_dict_up_to_metrics[metric_lower]['prearc_xlink:from']

                    elif (xlink_lower_metric_match == xlink_lower_metric) and (xlink_lower_metric_match in top_level_xlink_from):
                        
                        tmp_stock_dict = self.create_tmp_stock_dict(stock_dict_up_to_metrics[metric_lower],metric_lower)
                        tmp_stock_dict_list.append(tmp_stock_dict)

                        stock_list_dict[metric] = tmp_stock_dict_list 
                        break

                
                if (xlink_lower_metric_match not in top_level_xlink_from) and (while_count == WHILE_COUNT_CUTOFF): 
                    csv_logger.warning(f"Skipped {metric_lower} due to error in presentation file") 
                    break #GIVE UP ON FINDING MATCHES AT THIS POINT

                while_count+=1

        return stock_list_dict 
            



    def create_stock_dict_list(self,stock_dict_with_ded,statement,freq,csv_logger):
        stock_list_dict = dict()
        #pull out label chain for each numeric metric into dict
        for metric in stock_dict_with_ded[statement]['metrics']:
            if (freq in stock_dict_with_ded[statement]['metrics'][metric]) or ('instant' in stock_dict_with_ded[statement]['metrics'][metric]): 
                stock_list_dict = self.add_to_stock_list_dict(stock_dict_with_ded,stock_list_dict,statement,metric,stock_dict_with_ded[statement]['metrics'],csv_logger)
            if 'segment' in stock_dict_with_ded[statement]['metrics'][metric]: 
                for segment_metric in stock_dict_with_ded[statement]['metrics'][metric]['segment']:
                    if (freq in stock_dict_with_ded[statement]['metrics'][metric]['segment'][segment_metric]) or ('instant' in stock_dict_with_ded[statement]['metrics'][metric]['segment'][segment_metric]):   
                        sld_index = f"{metric}___{segment_metric}"
                        stock_list_dict = self.add_to_stock_list_dict(stock_dict_with_ded,stock_list_dict,statement,segment_metric,stock_dict_with_ded[statement]['metrics'][metric]['segment'],csv_logger,True,sld_index) #order segments just below parent item
                        if metric in stock_list_dict:
                            stock_list_dict[sld_index][0]['prearc_order'] = float(stock_list_dict[metric][0]['prearc_order']) + 0.1
                            for i in range(1,len(stock_list_dict[sld_index])):
                                stock_list_dict[sld_index][i]['prearc_order'] = stock_list_dict[metric][i]['prearc_order'] 
        return stock_list_dict                    


    def stock_list_dict_to_dataframe(self,stock_dict_with_ded,document_end_date,document_type,statement,freq,csv_logger):

        stock_list_dict = self.create_stock_dict_list(stock_dict_with_ded,statement,freq,csv_logger) 

        metric_list = []
        for row in stock_list_dict:
            metric_list.append(row)

        df_statement = pd.DataFrame(index=metric_list)  


        for metric in stock_list_dict:

            if 'labels' in stock_list_dict[metric][0]:
                label_keys =  stock_list_dict[metric][0]['labels'].keys()
                for key in label_keys:
                    df_statement.loc[df_statement.index==metric,key] =  stock_list_dict[metric][0]['labels'][key]  

            for period_type in [freq,'instant']: 
                if period_type in stock_list_dict[metric][0]:
                    df_statement.loc[df_statement.index==metric,'period_type'] = period_type 
                    for metric_date in stock_list_dict[metric][0][period_type]:
                        df_statement.loc[df_statement.index==metric,metric_date] =  stock_list_dict[metric][0][period_type][metric_date]
            
        df_statement = df_statement.reindex(sorted(df_statement.columns,reverse=True), axis=1)
        df_statement.index.name = statement

        if len(df_statement) > 0:
            self.save_statement(df_statement,document_end_date,document_type,statement,csv_logger)
        else: 
            csv_logger.warning(f"{statement}: had no numeric values and was not saved")


    def make_filename_safe(self,statement_name):
        keepcharacters = (' ','-','_')
        statement_name = "".join(c for c in statement_name if c.isalnum() or c in keepcharacters).rstrip()

        if len(statement_name) > 250:
            statement_name = statement_name[:250]

        return statement_name


    def save_statement(self,df_statement,document_end_date,document_type,statement,csv_logger):
        try:
            statement_name = self.stock_dict[document_end_date]['statements'][statement]['statement_name']
        except KeyError: 
            csv_logger.error(f"'statement_name' not found in self.stock_dict for {document_end_date} -> {statement}")
        else:
            statement_folder = f"{self.csv_path}/{document_end_date} ({document_type})/"
            Path(statement_folder).mkdir(parents=True, exist_ok=True)
            statement_name = self.make_filename_safe(statement_name) 
            df_statement.to_csv(f"{statement_folder}{statement_name}.csv")



    def extract_freq(self,document_end_date):
        if self.stock_dict[document_end_date]['document_type'] == '10-Q':  
            freq = 'qtd'
        elif self.stock_dict[document_end_date]['document_type'] in ['10-K']:  
            freq = 'ytd'

        return freq




##file_list = glob.glob(f"{json_path}/*.json") 
#file_list = [f"{json_path}ZG.json"]



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

