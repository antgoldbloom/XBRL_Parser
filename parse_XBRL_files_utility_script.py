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

from time import time

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
            elif (filename[-3:] == 'xsd'):
                soup_dict['xsd'] = create_soup_object(dirname,filename) 


        if soup_dict.keys() >= {"lab", "pre","instance","xsd"}:
            stock_dict = add_to_stock_dict(stock_dict,soup_dict,instance_filepath)
    
    return stock_dict
    


def add_metrics(soup,stock_dict_with_ded,context_dict,metric_list):
    
    tag_list = soup.find_all()
    for tag in tag_list:
        tag_name_str = convert_tag_name_str(tag.name)
        if tag_name_str in metric_list:
            statement = extract_statement_from_metric(tag_name_str,stock_dict_with_ded) 
            if (statement is not None) & (tag.has_attr('contextref')) & (tag.get_text().isnumeric() ): #file size becomes huge if you include non-numeric data
                contextref = tag['contextref']
                if ('segment' not in context_dict[contextref]): #checking it's not a revenue segment and just for one quarter           

                    if 'enddate' in context_dict[contextref]: 
                        stock_dict_with_ded= add_duration_metric(context_dict,contextref,stock_dict_with_ded,tag,statement)
                    
                    elif 'instant' in context_dict[contextref]: #balance sheet item
                        stock_dict_with_ded= add_instance_metric(context_dict,contextref,stock_dict_with_ded,tag,statement)
    
    return stock_dict_with_ded 


def extract_statement_from_metric(chosen_metric,stock_list_with_ded):
    for statement in stock_list_with_ded:
        for metric in stock_list_with_ded[statement]['metrics']:
            if metric.lower() == chosen_metric:
                return statement

    return None


def extract_metric_list(stock_dict_with_ded):
    metric_list = []
    for statement in stock_dict_with_ded:
        for metric in stock_dict_with_ded[statement]['metrics']:
            metric_list.append(metric.lower())

    return metric_list

def add_duration_metric(context_dict,contextref,stock_dict_with_ded,tag,statement):
    enddate = context_dict[contextref]['enddate']
    startdate = context_dict[contextref]['startdate']

    freq = "" 
    if days_between(startdate,enddate) > 94: #slightly nervous about assuming variables are either QTD or YTD. Has been empircally true for every company I've looked at so far though 
        freq = '_YTD'

    tag_name_str = convert_tag_name_str(tag.name) + freq

    #need these statements because of freq
    stock_dict_with_ded[statement] = create_dict_if_new_key('metrics', stock_dict_with_ded[statement])
    stock_dict_with_ded[statement]['metrics'] = create_dict_if_new_key(tag_name_str, stock_dict_with_ded[statement]['metrics'])

    stock_dict_with_ded[statement]['metrics'][tag_name_str] = create_dict_if_new_key('value', stock_dict_with_ded[statement]['metrics'][tag_name_str])
    stock_dict_with_ded[statement]['metrics'][tag_name_str]['value'] = create_dict_if_new_key(enddate, stock_dict_with_ded[statement]['metrics'][tag_name_str]['value'])
    stock_dict_with_ded[statement]['metrics'][tag_name_str]['value'][enddate] = tag.get_text()

    return stock_dict_with_ded

    
def add_instance_metric(context_dict,contextref,stock_dict_with_ded,tag,statement):
    instant = context_dict[contextref]['instant']

    tag_name_str = convert_tag_name_str(tag.name)

    stock_dict_with_ded[statement]['metrics'][tag_name_str] = create_dict_if_new_key('value', stock_dict_with_ded[statement]['metrics'][tag_name_str])
    stock_dict_with_ded[statement]['metrics'][tag_name_str]['value'] = create_dict_if_new_key(instant, stock_dict_with_ded[statement]['metrics'][tag_name_str]['value'])
    stock_dict_with_ded[statement]['metrics'][tag_name_str]['value'][instant] = tag.get_text()

    return stock_dict_with_ded


def days_between(d1, d2):
    d1 = datetime.strptime(d1, "%Y-%m-%d")
    d2 = datetime.strptime(d2, "%Y-%m-%d")
    return abs((d2 - d1).days)

    
def parse_pre_xml(soup_pre,stock_dict_with_ded):

    for statement_tag in soup_pre.find_all(['link:presentationlink','presentationlink']):
        statement_role_str = re.search('/role/[A-Za-z]+',statement_tag['xlink:role'])
        if statement_role_str is not None:
            statement_name = statement_role_str.group(0)[6:]
            stock_dict_with_ded[statement_name] = dict()
            stock_dict_with_ded[statement_name]['metrics'] = dict()
            pre_arc_dict = dict() #creating a dict for the presentation tags to store order and xlink from
            for metric_tag in statement_tag:
                if isinstance(metric_tag, Tag): #prevents pulling Navigatable String
                    if metric_tag.name in ['link:loc','loc']:
                        stock_dict_with_ded = parse_pre_loc_xml(metric_tag,stock_dict_with_ded,statement_name)
                    if metric_tag.name in ['link:presentationarc','presentationarc']: 
                        pre_arc_dict = extra_pre_arc_xml(metric_tag,pre_arc_dict) 
            for metric in stock_dict_with_ded[statement_name]['metrics']:
                loc_xlink_label = stock_dict_with_ded[statement_name]['metrics'][metric]['prearc_xlink:to'] 
                if loc_xlink_label in pre_arc_dict: 
                    stock_dict_with_ded[statement_name]['metrics'][metric]['prearc_order'] = pre_arc_dict[loc_xlink_label]['prearc_order']
                    stock_dict_with_ded[statement_name]['metrics'][metric]['prearc_xlink:from'] = pre_arc_dict[loc_xlink_label]['prearc_xlink:from']

    return stock_dict_with_ded
                        

def parse_pre_loc_xml(metric_tag,stock_dict_with_ded,statement_name):
    metric_str = metric_tag['xlink:href']
    metric = metric_str[metric_str.find('#')+1:].lower()
    stock_dict_with_ded[statement_name]['metrics'][metric] = dict()
    stock_dict_with_ded[statement_name]['metrics'][metric]['prearc_xlink:to'] = metric_tag['xlink:label'].lower()
    return stock_dict_with_ded 

def extra_pre_arc_xml(metric_tag,pre_arc_dict): 
    xlink_to = metric_tag['xlink:to'].lower() 
    pre_arc_dict[xlink_to] = dict() 
    pre_arc_dict[xlink_to]['prearc_xlink:from'] = metric_tag['xlink:from'].lower() 
    pre_arc_dict[xlink_to]['prearc_order'] = metric_tag['order'].lower() 
    return pre_arc_dict


def parse_lab_xml(soup_lab,stock_dict_with_ded,metric_list):
    
    

    tag_list = soup_lab.find_all()

    lab_loc_arc_dict = dict()
    labelarc_dict = dict() 
    linklabel_dict = dict() 

    for tag in tag_list:

        if tag.name in ['link:loc','loc']:
            metric_str = tag['xlink:href']
            metric = metric_str[metric_str.find('#')+1:].lower()

            if metric in metric_list:
                statement_name = extract_statement_from_metric(metric,stock_dict_with_ded) #INVESIGATE does the same metric show up in multiple statements?
                
                if statement_name is not None:
                    lab_loc_arc_dict = create_dict_if_new_key(statement_name,lab_loc_arc_dict)
                    lab_loc_arc_dict[statement_name][metric] = tag['xlink:label']  

        elif tag.name in ['link:labelarc','labelarc']:
#            if tag['xlink:from'] == 'loc_us-gaap_StatementOfFinancialPositionAbstract_D8B71A20A71BCC3CB4D050307C091C11':
#                print('here')
            labelarc_dict[tag['xlink:from']] = tag['xlink:to'] 
        elif tag.name in ['link:label','label']:
            linklabel_dict[tag['xlink:label']] = tag.get_text()  

    stock_dict_with_ded = add_lab_to_stock_dict(lab_loc_arc_dict,labelarc_dict,linklabel_dict,stock_dict_with_ded)

    return stock_dict_with_ded 
                        

def add_lab_to_stock_dict(lab_loc_arc_dict,labelarc_dict,linklabel_dict,stock_dict_with_ded):
    for statement_name in lab_loc_arc_dict:
        for metric in lab_loc_arc_dict[statement_name]:
            try:
                tmp_labelarc_xlink_to = labelarc_dict[lab_loc_arc_dict[statement_name][metric]] 
                stock_dict_with_ded[statement_name]['metrics'][metric]['label'] = linklabel_dict[tmp_labelarc_xlink_to] 
            except KeyError as err:
                print(f"Couldn't add label: {err}")
    return stock_dict_with_ded

def add_to_stock_dict(stock_dict,soup_dict,instance_filepath):

    document_end_date = None 
    try:
        document_end_date = soup_dict['instance'].find('dei:documentperiodenddate').get_text()
    except: 
        print('dei:documentperiodenddate not found in {}'.format(instance_filepath))
              
    if document_end_date is not None:

        stock_dict[document_end_date] = dict()

        #Add from Presentation file
        stock_dict[document_end_date] = parse_pre_xml(soup_dict['pre'],stock_dict[document_end_date])

        #this metric list is used in pre_lab_xml and add_metrics
        metric_list = extract_metric_list(stock_dict[document_end_date]) 

        #Add from label file
        stock_dict[document_end_date] = parse_lab_xml(soup_dict['lab'],stock_dict[document_end_date],metric_list)

        #Add from XSD file
        stock_dict[document_end_date] = parse_xsd(soup_dict['xsd'],stock_dict[document_end_date],document_end_date)

        #Add from instance file
        context_dict = do_context_mapping(soup_dict['instance'])
        stock_dict[document_end_date] = add_metrics(soup_dict['instance'],stock_dict[document_end_date],context_dict,metric_list)    
            
    return stock_dict
    
def parse_xsd(soup_xsd,stock_dict_with_ded,document_end_date):

    for statement_tag in soup_xsd.find_all(['link:roletype','roletype']):
        for statement_sub_tag in statement_tag:
            if statement_sub_tag.name in ['link:definition','definition']: 
                if statement_tag['id'] in stock_dict_with_ded:
                    stock_dict_with_ded[statement_tag['id']]['statement_name'] = statement_sub_tag.get_text() 

    return stock_dict_with_ded

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
    


def extract_gap_tag_list(soup):

    us_gaap_tags = soup.find_all(re.compile("^us-gaap:"))
    tags_by_statement_dict = []
    for tag in us_gaap_tags:
        tags_by_statement_dict.append(tag.name)

    tags_by_statement_dict = np.unique(tags_by_statement_dict)
    
    return tags_by_statement_dict   




                  
    




####UTILITY FUNCTIONS

def convert_str_name_tag(str_name):
    return str_name.replace('_',':')

def convert_tag_name_str(tag_name):
    return tag_name.replace(':','_')

def create_dict_if_new_key(s_key,s_dict):
    if s_key not in s_dict:
        s_dict[s_key] = dict()
    return s_dict

def create_soup_object(dirname,filename):
    with open(os.path.join(dirname, filename), "r") as f:
        contents = f.read()
        soup = BeautifulSoup(contents, 'lxml')
    return soup

