import json
import numpy as np
import pandas as pd
import os

json_path = '../data/json/'
csv_path = '../data/csv/'

ticker = 'NVDA'
document_end_date = '2020-01-26'
statement_name = 'ConsolidatedStatementsOfIncome' 
#statement_name = 'SegmentInformationReportableSegmentsAndReconciliationToConsolidatedNetIncomeDetails'
#statement_name = 'SegmentInformationDisaggregationOfRevenueDetails'
 
def load_json_to_dict(json_path,ticker):

    with open(f"{json_path}{ticker}.json", 'r') as stock_json:
        stock_dict = json.loads(stock_json.read())

    return stock_dict[document_end_date]

def list_top_level_statement_tags(stock_dict_with_ded):
    top_level_xlink_from = [] 
    #idetify top metric_label
    for metric in stock_dict_with_ded[statement_name]['metrics']:
        metric_dict_slice = stock_dict_with_ded[statement_name]['metrics'][metric] 
        if ('prearc_xlink:from' not in metric_dict_slice): 
            top_level_xlink_from.append(stock_dict_with_ded[statement_name]['metrics'][metric]['prearc_xlink:to'])
    return top_level_xlink_from

def create_tmp_stock_dict(stock_dict_with_ded_statement_metric,metric):
    dict_key_list = ['label','prearc_order', 'prearc_xlink:from','prearc_xlink:to','qtd','ytd','instant']
    tmp_stock_dict = dict()
    tmp_stock_dict['metric'] = metric 
    for key in dict_key_list:
        if key in stock_dict_with_ded_statement_metric: 
            tmp_stock_dict[key] = stock_dict_with_ded_statement_metric[key]
    return tmp_stock_dict


def add_to_stock_list_dict(stock_list_dict,metric,stock_dict_up_to_metrics,is_segment=False,parent_metric=None):
    tmp_stock_dict_list = []
    top_level_xlink_from = list_top_level_statement_tags(stock_dict_with_ded) 
    tmp_stock_dict = create_tmp_stock_dict(stock_dict_up_to_metrics[metric],metric)
    tmp_stock_dict_list.append(tmp_stock_dict)

    if not is_segment:
        stock_list_dict = walk_prearc_xlink_tree(top_level_xlink_from,metric,stock_dict_up_to_metrics, stock_list_dict,tmp_stock_dict_list)  
    else: # this is for segments
        stock_list_dict[sld_index] = tmp_stock_dict_list 

    return stock_list_dict


def walk_prearc_xlink_tree(top_level_xlink_from,metric,stock_dict_up_to_metrics, stock_list_dict,tmp_stock_dict_list): 
    xlink_lower_metric = stock_dict_up_to_metrics[metric]['prearc_xlink:from']
    xlink_lower_metric_match = None 

    while (xlink_lower_metric_match not in top_level_xlink_from):
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

    return stock_list_dict 
        



def create_stock_dict_list(stock_dict_with_ded,freq):
    stock_list_dict = dict()
    #pull out label chain for each numeric metric into dict
    for metric in stock_dict_with_ded[statement_name]['metrics']:
        if (freq in stock_dict_with_ded[statement_name]['metrics'][metric]) or ('instant' in stock_dict_with_ded[statement_name]['metrics'][metric]): 
            stock_list_dict = add_to_stock_list_dict(stock_list_dict,metric,stock_dict_with_ded[statement_name]['metrics'])
        if 'segment' in stock_dict_with_ded[statement_name]['metrics'][metric]: 
            for segment_metric in stock_dict_with_ded[statement_name]['metrics'][metric]['segment']:
                if (freq in stock_dict_with_ded[statement_name]['metrics'][metric]['segment'][segment_metric]) or ('instant' in stock_dict_with_ded[statement_name]['metrics'][metric]['segment'][segment_metric]): 
                    sld_index = f"{metric}___{segment_metric}"
                    stock_list_dict = add_to_stock_list_dict(stock_list_dict,segment_metric,stock_dict_with_ded[statement_name]['metrics'][metric]['segment'],True,sld_index) #order segments just below parent item
                    stock_list_dict[sld_index][0]['prearc_order'] = int(stock_list_dict[metric][0]['prearc_order']) + 0.1
                    for i in range(1,len(stock_list_dict[sld_index])):
                        stock_list_dict[sld_index][i]['prearc_order'] = stock_list_dict[metric][i]['prearc_order'] 
    return stock_list_dict                    


def stock_list_dict_to_dataframe(stock_dict_with_ded,freq):

    stock_list_dict = create_stock_dict_list(stock_dict_with_ded,freq) 

    metric_list = []
    for row in stock_list_dict:
        metric_list.append(row)

    df_statement = pd.DataFrame(index=metric_list)  


    for metric in stock_list_dict:
        
        if freq in stock_list_dict[metric][0]:
            for metric_date in stock_list_dict[metric][0][freq]:
                df_statement.loc[df_statement.index==metric,metric_date] =  stock_list_dict[metric][0][freq][metric_date]
        elif 'instant' in stock_list_dict[metric][0]:     
            for metric_date in stock_list_dict[metric][0]['instant']:
                df_statement.loc[df_statement.index==metric,metric_date] =  stock_list_dict[metric][0]['instant'][metric_date]

        
        
    df_statement = df_statement.iloc[:, ::-1] #reverse order of dates
    return df_statement

stock_dict_with_ded = load_json_to_dict(json_path,ticker)
df_statement_qtd = stock_list_dict_to_dataframe(stock_dict_with_ded,'qtd')
print(df_statement_qtd)
df_statement_ytd = stock_list_dict_to_dataframe(stock_dict_with_ded,'ytd')
print(df_statement_ytd)
df_statement_instant = stock_list_dict_to_dataframe(stock_dict_with_ded,'instant')

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

