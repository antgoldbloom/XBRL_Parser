# This Python 3 environment comes with many helpful analytics libraries installed
# It is defined by the kaggle/python docker image: https://github.com/kaggle/docker-python
# For example, here's several helpful packages to load in 

import numpy as np # linear algebra
import pandas as pd # data processing, CSV file I/O (e.g. pd.read_csv)

import json

from bs4 import BeautifulSoup, Tag
import re

import requests
import sys

import shutil

import os

from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta

import timeit

pd.set_option('max_rows',500)

def create_stock_dict(ticker,file_directory):
    
    stock_dict = dict()
    
    for dirname, _, filenames in os.walk('{}{}/'.format(file_directory,ticker)):
        soup_dict = dict()
        for filename in filenames:
            if (filename[-7:-4] in ['lab','pre']):
                soup_dict[filename[-7:-4]] = create_soup_object(dirname,filename) 
            elif (filename[-3:] == 'xml'):
                soup_dict['instance'] = create_soup_object(dirname,filename) 
                instance_filepath = os.path.join(dirname,filename)

        if soup_dict.keys() >= {"lab", "pre","instance"}:
            stock_dict = add_to_stock_dict(stock_dict,soup_dict,instance_filepath)
    
    return stock_dict
    

def create_soup_object(dirname,filename):
    with open(os.path.join(dirname, filename), "r") as f:
        contents = f.read()
        soup = BeautifulSoup(contents, 'lxml')
    return soup

        
def add_metrics(soup,stock_dict,context_dict,tags_by_statement_dict,document_end_date):

    stock_dict[document_end_date] = dict()
    
    metric_list = extract_metric_list(tags_by_statement_dict)
    
    tag_list = soup.find_all()
    for tag in tag_list:
        tag_name_str = convert_tag_name_str(tag.name)
        if tag_name_str in metric_list:
            statement = extract_statement_from_metric(tag_name_str,tags_by_statement_dict) 

            if (statement is not None) & (tag.has_attr('contextref')) & (tag.get_text().isnumeric() ): #file size becomes huge if you include non-numeric data

                stock_dict[document_end_date] = create_dict_if_new_key(statement,stock_dict[document_end_date])

                contextref = tag['contextref']
                if ('segment' not in context_dict[contextref]): #checking it's not a revenue segment and just for one quarter           

                    if 'enddate' in context_dict[contextref]: 
                        stock_dict = add_duration_metric(context_dict,contextref,stock_dict,tags_by_statement_dict,tag,document_end_date,statement)
                        
                    elif 'instant' in context_dict[contextref]: #balance sheet item
                        stock_dict= add_instance_metric(context_dict,contextref,stock_dict,tags_by_statement_dict,tag,document_end_date,statement)
    
    return stock_dict


def extract_statement_from_metric(chosen_metric,tags_by_statement_dict):
    for statement in tags_by_statement_dict:
        for metric in tags_by_statement_dict[statement]:
            if metric.lower() == chosen_metric:
                return statement

    return None


def extract_metric_list(tags_by_statement_dict):
    metric_list = []
    for statement in tags_by_statement_dict:
        for metric in tags_by_statement_dict[statement]:
            metric_list.append(metric.lower())

    return metric_list

def convert_tag_name_str(tag_name):
    return tag_name.replace(':','_')

def create_dict_if_new_key(s_key,s_dict):
    if s_key not in s_dict:
        s_dict[s_key] = dict()
    return s_dict

def add_duration_metric(context_dict,contextref,stock_dict,tags_by_statement_dict,tag,document_end_date,statement):
    enddate = context_dict[contextref]['enddate']
    startdate = context_dict[contextref]['startdate']

    tag_name_str = convert_tag_name_str(tag.name)

    stock_dict[document_end_date][statement] = create_dict_if_new_key(tag_name_str,stock_dict[document_end_date][statement])
    stock_dict[document_end_date][statement][tag_name_str] = create_dict_if_new_key(enddate,stock_dict[document_end_date][statement][tag_name_str])

    if days_between(startdate,enddate) < 95:
        freq = 'QTD'
    else:
        freq = 'YTD'

    stock_dict[document_end_date][statement][tag_name_str][enddate] = create_dict_if_new_key(freq,stock_dict[document_end_date][statement][tag_name_str][enddate])

    stock_dict[document_end_date][statement][tag_name_str][enddate][freq]['value'] = tag.get_text()
    stock_dict[document_end_date][statement][tag_name_str][enddate][freq]['statedate'] = startdate 

    stock_dict[document_end_date][statement][tag_name_str][enddate][freq]['label'] = tags_by_statement_dict[statement][tag_name_str]['label']
    stock_dict[document_end_date][statement][tag_name_str][enddate][freq]['order'] = tags_by_statement_dict[statement][tag_name_str]['order']
    stock_dict[document_end_date][statement][tag_name_str][enddate][freq]['link_from'] = tags_by_statement_dict[statement][tag_name_str]['link_from']
    
    return stock_dict
    
def add_instance_metric(context_dict,contextref,stock_dict,tags_by_statement_dict,tag,document_end_date,statement):
    instant = context_dict[contextref]['instant']

    tag_name_str = convert_tag_name_str(tag.name)


    stock_dict[document_end_date][statement] = create_dict_if_new_key(tag_name_str,stock_dict[document_end_date][statement])
    stock_dict[document_end_date][statement][tag_name_str] = create_dict_if_new_key(instant,stock_dict[document_end_date][statement][tag_name_str])

    stock_dict[document_end_date][statement][tag_name_str][instant]['value'] = tag.get_text()

    stock_dict[document_end_date][statement][tag_name_str][instant]['label'] = tags_by_statement_dict[statement][tag_name_str]['label']
    stock_dict[document_end_date][statement][tag_name_str][instant]['order'] = tags_by_statement_dict[statement][tag_name_str]['order']
    stock_dict[document_end_date][statement][tag_name_str][instant]['link_from'] = tags_by_statement_dict[statement][tag_name_str]['link_from']

    return stock_dict


def days_between(d1, d2):
    d1 = datetime.strptime(d1, "%Y-%m-%d")
    d2 = datetime.strptime(d2, "%Y-%m-%d")
    return abs((d2 - d1).days)

    
def parse_pre_xml(soup_pre,tags_by_statement_dict):

    for statement_tag in soup_pre.find_all(['link:presentationlink','presentationlink']):
        statement_role_str = re.search('/role/[A-Za-z]+',statement_tag['xlink:role'])
        if statement_role_str is not None:
            statement_name = statement_role_str.group(0)[6:]
            tags_by_statement_dict[statement_name] = dict()
            for metric_tag in statement_tag:
                if isinstance(metric_tag, Tag): #prevents pulling Navigatable String
                    if metric_tag.name in ['link:presentationarc','presentationarc']:
                        metric = metric_tag['xlink:to'].lower()
                        tags_by_statement_dict[statement_name][metric] = dict()
                        tags_by_statement_dict[statement_name][metric]['order'] = metric_tag['order']
                        tags_by_statement_dict[statement_name][metric]['link_from'] = metric_tag['xlink:from'].lower()

    return tags_by_statement_dict
                        

def parse_lab_xml(soup_lab,tags_by_statement_dict):
    for label_tag in soup_lab.find_all(['link:label','label']):
        for statement_name in tags_by_statement_dict:
            metric = label_tag['xlink:label'][:-4].lower()
            if metric in tags_by_statement_dict[statement_name]:
                tags_by_statement_dict[statement_name][metric]['label'] = label_tag.text

    return tags_by_statement_dict
                        


def add_to_stock_dict(stock_dict,soup_dict,instance_filepath):
    
    context_dict = do_context_mapping(soup_dict['instance'])
    tags_by_statement_dict = extract_tag_dict(soup_dict['pre'],soup_dict['lab'])
    stock_dict = extract_document_end_date_and_add_metrics(soup_dict['instance'],stock_dict,context_dict,tags_by_statement_dict,instance_filepath)
            
    return stock_dict

def do_context_mapping(soup):

    context_dict = dict()
    context = soup.find_all(['context','xbrli:context'])

    for c in context:
        context_dict[c['id']] = dict() 

        for tag in ['startdate','enddate','segment','instant','xbrli:startdate','xbrli:enddate','xbrli:segment','xbrli:instant']:

            tag_str = tag #want to strip out xbrli: 
            if tag[0:6] == 'xbrli:':
                tag_str = tag[6:]
            
            if c.find(tag):
                context_dict[c['id']][tag_str] = c.find(tag).get_text()
            
    return context_dict
    

def extract_tag_dict(soup_pre,soup_lab):

    tags_by_statement_dict = dict()
    tags_by_statement_dict = parse_pre_xml(soup_pre,tags_by_statement_dict)
    tags_by_statement_dict = parse_lab_xml(soup_lab,tags_by_statement_dict)

    return tags_by_statement_dict
    

def extract_gap_tag_list(soup):

    us_gaap_tags = soup.find_all(re.compile("^us-gaap:"))
    tags_by_statement_dict = []
    for tag in us_gaap_tags:
        tags_by_statement_dict.append(tag.name)

    tags_by_statement_dict = np.unique(tags_by_statement_dict)
    
    return tags_by_statement_dict   


def extract_document_end_date_and_add_metrics(soup,stock_dict,context_dict,tags_by_statement_dict,instance_filepath):

    document_end_date = None 
    
    try:
        document_end_date = soup.find('dei:documentperiodenddate').get_text()
        
    except: 
        print('dei:documentperiodenddate not found in {}'.format(instance_filepath))
              
    if document_end_date is not None:
        stock_dict = add_metrics(soup,stock_dict,context_dict,tags_by_statement_dict,document_end_date)    
                  
    return stock_dict
    


def fill_QTD_drop_YTD(df):
    
    drop_YTD_list = []

    for column in df.columns:
        if (re.match('.+_QTD$',column)) and ('{}_YTD'.format(column[:-4]) in df.columns):
            drop_YTD_list.append('{}_YTD'.format(column[:-4]))

            for index in df[df[column].isna() & df['{}_YTD'.format(column[:-4])].notna()][[column,'{}_YTD'.format(column[:-4])]].index:
                df_tmp = df[df.index < index].tail(3)
                if (len(df_tmp[column].dropna()) == 3):

                    df.loc[df.index == index,column] = pd.to_numeric(df[df.index == index]['{}_YTD'.format(column[:-4])]) - pd.to_numeric(df_tmp[column]).sum()


    df = df.drop(columns=np.unique(drop_YTD_list))

    return df


def dict_to_dataframe(stock_dict):

    df = pd.DataFrame(index=stock_dict.keys())
        
    for enddate in stock_dict:
        for metric in stock_dict[enddate]:
            df.loc[df.index == enddate,metric] = stock_dict[enddate][metric]   
    
    df = df.sort_index(ascending=True)
    
    df = fill_QTD_drop_YTD(df)
    
    return df


def create_stock_dataframe(ticker):

        start = timeit.default_timer()        

        stock_dict = create_stock_dict(ticker)
        
        df = dict_to_dataframe(stock_dict)
        print('process: ', timeit.default_timer() - start)        
        
        return df




