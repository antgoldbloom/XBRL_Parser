
import numpy as np # linear algebra
import pandas as pd # data processing, CSV file I/O (e.g. pd.read_csv)

import json

from bs4 import BeautifulSoup, Tag
from lxml import etree
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
        parsed_xml_dict = dict()
        for filename in filenames:
            if (filename[-7:-4] == 'pre'):
                parsed_xml_dict['pre'] = create_soup_object(dirname,filename) 
            elif (filename[-7:-4] == 'lab'):
                parsed_xml_dict['lab']= etree.parse(os.path.join(dirname,filename))
            elif (filename[-7:-4] in ['cal']):
                parsed_xml_dict['cal']= etree.parse(os.path.join(dirname,filename))
            elif (filename[-3:] == 'xml'):
                instance_filepath = os.path.join(dirname,filename)
                parsed_xml_dict['instance']= etree.parse(instance_filepath)
            elif (filename[-3:] == 'xsd'):
                parsed_xml_dict['xsd'] = create_soup_object(dirname,filename) 


        
        if parsed_xml_dict.keys() >= {"lab", "pre","instance","xsd","cal"}:
            stock_dict = add_to_stock_dict(stock_dict,parsed_xml_dict,instance_filepath)
        elif re.match('[0-9]{4}-[0-9]{2}-[0-9]{2}',dirname[dirname.rfind('/')+1:]): #if in a directory with a date, print a wanring
            print(f"Warning: missing XBRL file for {ticker} in {dirname}")
    
    return stock_dict
    

def correct_truncated_tag_name(tag_name_str,metric_list):
    for metric in metric_list:
        if len(metric) > 100:
            if tag_name_str == metric[:100]:
                tag_name_str = metric
    return tag_name_str


def fetch_metrics_tag_list(parsed_xml):
    xpath_query_list = []
    parsed_xml = parsed_xml.getroot()
    for key in parsed_xml.nsmap:
        if key not in [None,'dei','iso4217','link','srt','xbrldi','xlink','xsi']:
            xpath_query_list.append(key)

    query_str = ('|'.join([f"{x}:*" for x in xpath_query_list]))

    ns_dict = parsed_xml.nsmap
    if None in ns_dict:
        del(ns_dict[None])
    
    return parsed_xml.xpath(f"//{query_str}",namespaces=ns_dict) 


def add_metrics(parsed_xml,stock_dict_with_ded,context_dict,metric_list,cal_dict):

    tag_list = fetch_metrics_tag_list(parsed_xml)

    for tag in tag_list:
        tag_name_str = f"{tag.prefix}_{etree.QName(tag).localname}".lower()
        ###DEBUG broken tags
        
        #if tag_name_str == "us-gaap_revenuefromcontractwithcustomerexcludingassessedtax":
        #    print(tag_name_str)

        if tag_name_str in metric_list:
            statement_list = extract_statement_from_metric(tag_name_str,stock_dict_with_ded) 
            if (len(statement_list) > 0) & ('contextRef' in tag.attrib) & (tag.text is not None):
                if (re.match('^-?[0-9]+\.?[0-9]*$',tag.text) ): #testin for numbers. file size becomes huge if you include non-numeric data. lstrip to prevent return false for negative numbers
                    contextref = tag.attrib['contextRef']

                    for statement in statement_list:


                        stock_dict_with_ded[statement]['metrics'] = create_dict_if_new_key(tag_name_str, stock_dict_with_ded[statement]['metrics']) 

                        hasSegment = False
                        dontAdd = False
                        if ('segment' in context_dict[contextref]): #checking it's a segment and just for one quarter           
                            if convert_tag_name_str(context_dict[contextref]['segment']) in stock_dict_with_ded[statement]['metrics']: #checking if segment is in this statement
                                stock_dict_with_ded[statement]['metrics'][tag_name_str] = create_dict_if_new_key('segment', stock_dict_with_ded[statement]['metrics'][tag_name_str]) 
                                segment_name_str = convert_tag_name_str(context_dict[contextref]['segment']) 
                                stock_dict_with_ded[statement]['metrics'][tag_name_str]['segment'] = create_dict_if_new_key(segment_name_str, stock_dict_with_ded[statement]['metrics'][tag_name_str]['segment']) 
                                hasSegment = True 
                            else:
                                dontAdd = True #segment not in this statement, don't let past the endDate or instant qualifiers

                        if 'endDate' in context_dict[contextref] and dontAdd == False: 
                            if hasSegment:
                                stock_dict_with_ded[statement]['metrics'][tag_name_str]['segment'][context_dict[contextref]['segment']] = add_duration_metric(stock_dict_with_ded[statement]['metrics'][tag_name_str]['segment'][segment_name_str],context_dict,contextref,tag,tag_name_str,statement,cal_dict)
                            else:
                                stock_dict_with_ded[statement]['metrics'][tag_name_str] = add_duration_metric(stock_dict_with_ded[statement]['metrics'][tag_name_str],context_dict,contextref,tag,tag_name_str,statement,cal_dict)

                        elif 'instant' in context_dict[contextref] and dontAdd == False: #balance sheet item
                            if hasSegment:
                                stock_dict_with_ded[statement]['metrics'][tag_name_str]['segment'][context_dict[contextref]['segment']] = add_value_to_metric(stock_dict_with_ded[statement]['metrics'][tag_name_str]['segment'][segment_name_str],tag,context_dict[contextref]['instant'],cal_dict,tag_name_str,statement,'instant') 
                            else:
                                stock_dict_with_ded[statement]['metrics'][tag_name_str] = add_value_to_metric(stock_dict_with_ded[statement]['metrics'][tag_name_str],tag,context_dict[contextref]['instant'],cal_dict,tag_name_str,statement,'instant') 
    
    return stock_dict_with_ded 


def extract_statement_from_metric(chosen_metric,stock_list_with_ded):
    statement_list = []
    for statement in stock_list_with_ded:
        for metric in stock_list_with_ded[statement]['metrics']:

            if metric.lower() == chosen_metric:
                statement_list.append(statement)

    return statement_list 


def extract_metric_list(stock_dict_with_ded):
    metric_list = []
    for statement in stock_dict_with_ded:
        for metric in stock_dict_with_ded[statement]['metrics']:
            metric_list.append(metric.lower())

    return metric_list

def add_duration_metric(stock_dict_excl_value_and_date,context_dict,contextref,tag,tag_name_str,statement,cal_dict):
    enddate = context_dict[contextref]['endDate']
    startdate = context_dict[contextref]['startDate']

    freq = "qtd" 
    if days_between(startdate,enddate) > 94: #slightly nervous about assuming variables are either QTD or YTD. Has been empircally true for every company I've looked at so far though 
        freq = 'ytd'

    #need these statements because of freq
    #stock_dict_with_ded[statement] = create_dict_if_new_key('metrics', stock_dict_with_ded[statement])
    #stock_dict_with_ded[statement]['metrics'] = create_dict_if_new_key(tag_name_str, stock_dict_with_ded[statement]['metrics'])
    stock_dict_excl_value_and_date = add_value_to_metric(stock_dict_excl_value_and_date,tag,enddate,cal_dict,tag_name_str,statement,freq) 

    return stock_dict_excl_value_and_date 



    
def add_instance_metric(context_dict,contextref,stock_dict_with_ded,tag,tag_name_str,statement,cal_dict):
    instant = context_dict[contextref]['instant']

    stock_dict_with_ded[statement]['metrics'][tag_name_str] = add_value_to_metric(stock_dict_with_ded[statement]['metrics'][tag_name_str],tag,instant,cal_dict,tag_name_str,statement,'instant') 

    return stock_dict_with_ded


def add_value_to_metric(stock_dict_excl_value_and_date,tag,obs_date,cal_dict,tag_name_str,statement,freq):
    stock_dict_excl_value_and_date = create_dict_if_new_key(freq, stock_dict_excl_value_and_date)
    stock_dict_excl_value_and_date[freq] = create_dict_if_new_key(obs_date, stock_dict_excl_value_and_date[freq])
    try:
        stock_dict_excl_value_and_date[freq][obs_date] = int(tag.text)
    except:
        stock_dict_excl_value_and_date[freq][obs_date] = float(tag.text)

    #look at the sign of the metric
    if statement in cal_dict: 
        if tag_name_str in cal_dict[statement]: 
            stock_dict_excl_value_and_date[freq][obs_date] = stock_dict_excl_value_and_date[freq][obs_date] * cal_dict[statement][tag_name_str]

    return stock_dict_excl_value_and_date

def days_between(d1, d2):
    d1 = datetime.strptime(d1, "%Y-%m-%d")
    d2 = datetime.strptime(d2, "%Y-%m-%d")
    return abs((d2 - d1).days)

    
def parse_pre_xml(soup_pre,stock_dict_with_ded):

    """
    link:loc role xlink:label  
    link:presentationArc xlink:to xlink:from
    link:loc xlink:from xlink:label 
    """

    for statement_tag in soup_pre.find_all(['link:presentationlink','presentationlink']):


        statement_role_str = re.search('/role/.+$',statement_tag['xlink:role'])
        if statement_role_str is not None:
            statement_name = statement_role_str.group(0)[6:].lower()
            stock_dict_with_ded[statement_name] = dict()
            stock_dict_with_ded[statement_name]['metrics'] = dict()
            pre_arc_dict = dict() #creating a dict for the presentation tags to store order and xlink from

            ###DEBUG
            #if statement_name == 'CONSOLIDATEDSTATEMENTSOFINCOME':
            #    print(statement_tag)

            for metric_tag in statement_tag:
                if isinstance(metric_tag, Tag): #prevents pulling Navigatable String
                    if metric_tag.name in ['link:loc','loc']:
                        stock_dict_with_ded = parse_pre_loc_xml(metric_tag,stock_dict_with_ded,statement_name)
                    if metric_tag.name in ['link:presentationarc','presentationarc']: 
                        pre_arc_dict = extract_pre_arc_xml(metric_tag,pre_arc_dict) 

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

def extract_pre_arc_xml(metric_tag,pre_arc_dict): 
    xlink_to = metric_tag['xlink:to'].lower() 
    pre_arc_dict[xlink_to] = dict() 
    pre_arc_dict[xlink_to]['prearc_xlink:from'] = metric_tag['xlink:from'].lower() 
    pre_arc_dict[xlink_to]['prearc_order'] = metric_tag['order'].lower() 
    return pre_arc_dict

def parse_cal_xml(parsed_xml):
    CALC_NAMESPACES = {"xlink": "http://www.w3.org/1999/xlink","link":"http://www.xbrl.org/2003/linkbase"}

    loc_list = parsed_xml.xpath("//*[name()='link:loc']",namespaces=CALC_NAMESPACES)

    cal_dict = dict()


    for element in loc_list:

        #metric_str = element.attrib['{http://www.w3.org/1999/xlink}href'] 
        metric_href = element.xpath('@xlink:href',namespaces=CALC_NAMESPACES)[0]
        metric = metric_href[metric_href.find('#')+1:].lower()

        lab_str = element.xpath('@xlink:label',namespaces=CALC_NAMESPACES)[0] 

        calculation_element = parsed_xml.xpath(f"//*[@xlink:to='{lab_str}']",namespaces=CALC_NAMESPACES) 
        for ce in calculation_element: 
            calculation_link_element = ce.getparent()
            calculation_link_element_role = calculation_link_element.xpath("@xlink:role",namespaces=CALC_NAMESPACES)[0] 
            statement_role_str = re.search('/role/[A-Za-z]+',calculation_link_element_role).group(0)[6:] 
            if statement_role_str not in cal_dict:
                cal_dict[statement_role_str] = dict()
        
            cal_dict[statement_role_str][metric] = float(calculation_element[0].xpath('@weight')[0]) 

    return cal_dict

def create_label_lookup(parsed_xml):

    LABEL_NAMESPACE = {'link': 'http://www.xbrl.org/2003/linkbase', 'xsi': 'http://www.w3.org/2001/XMLSchema-instance', 'xlink': 'http://www.w3.org/1999/xlink'}

    parsed_xml = parsed_xml.getroot()
    #ns_dict = parsed_xml.nsmap

    parsed_xml_loc = parsed_xml.xpath(f"//link:loc",namespaces=LABEL_NAMESPACE)

    lab_dict = {}


    for loc in parsed_xml_loc:
        metric_href_elements = loc.xpath('@xlink:href',namespaces=LABEL_NAMESPACE)
        for metric_href in metric_href_elements:
            label = loc.xpath('@xlink:label',namespaces=LABEL_NAMESPACE)[0]
            metric = metric_href[metric_href.find('#')+1:].lower()
            if metric not in lab_dict:
                lab_dict[metric] = {}
        
            lab_dict[metric]['xlink:label'] = label 
            lab_dict[metric]['label'] = {} 

    parsed_xml_lab = parsed_xml.xpath(f"//link:label",namespaces=LABEL_NAMESPACE)

    for lab in parsed_xml_lab:
        for metric in lab_dict:
            xlink_label = lab_dict[metric]['xlink:label']
            if xlink_label[4:] == lab.get(f"{{{LABEL_NAMESPACE['xlink']}}}label")[4:]: 
                label_role = lab.get(f"{{{LABEL_NAMESPACE['xlink']}}}role")
                label_role = re.search('/role/[A-Za-z]+',label_role).group(0)[6:] 
                lab_dict[metric]['label'][label_role] = lab.text


    return lab_dict




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
                statement_list = extract_statement_from_metric(metric,stock_dict_with_ded) #INVESIGATE does the same metric show up in multiple statements?
                
                if len(statement_list) > 0:
                    for statement_name in statement_list:
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

def add_to_stock_dict(stock_dict,parsed_xml_dict,instance_filepath):

    document_end_date = None 
    try:
        document_end_date = parsed_xml_dict['instance'].xpath("//*[local-name()='DocumentPeriodEndDate']")[0].text
        #.find('dei:documentperiodenddate').get_text()
    except: 
        print('dei:documentperiodenddate not found in {}'.format(instance_filepath))
              
    if document_end_date is not None:

        stock_dict[document_end_date] = dict()
        #stock_dict[document_end_date]['document_type'] = dict()

        stock_dict[document_end_date]['document_type'] = parsed_xml_dict['instance'].xpath("//*[local-name()='DocumentType']")[0].text 

        stock_dict[document_end_date]['statements'] = dict()
        """
            This the stock_dict hierarchy
            document date -> Statement  -> 'metric' -> metric  -> order 
            document date -> Statement  -> 'metric' -> metric  -> link from
            document date -> Statement  -> 'metric' -> metric  -> link to 
            document date - > Statement -> 'metric' -> metric ->  'value' -> date
            document date -> Statement -> 'metric' -> metric -> label  
            document date -> statement -> 'statement_name' -> statement name  
            """


        #Add from Presentation file
        stock_dict[document_end_date]['statements'] = parse_pre_xml(parsed_xml_dict['pre'],stock_dict[document_end_date]['statements'])

        #this metric list is used in pre_lab_xml and add_metrics
        metric_list = extract_metric_list(stock_dict[document_end_date]['statements']) 

        #Add from label file
        #stock_dict[document_end_date]['statements'] = parse_lab_xml(parsed_xml_dict['lab'],stock_dict[document_end_date]['statements'],metric_list)
        label_lookup_dict = create_label_lookup(parsed_xml_dict['lab'])

        #Add from XSD file
        stock_dict[document_end_date]['statements'] = parse_xsd(parsed_xml_dict['xsd'],stock_dict[document_end_date]['statements'],document_end_date)

        #Add from instance file
        cal_dict = parse_cal_xml(parsed_xml_dict['cal'])
        context_dict = do_context_mapping(parsed_xml_dict['instance'])
        stock_dict[document_end_date]['statements'] = add_metrics(parsed_xml_dict['instance'],stock_dict[document_end_date]['statements'],context_dict,metric_list,cal_dict)    

            
    return stock_dict 

def parse_xsd(soup_xsd,stock_dict_with_ded,document_end_date):

    for statement_tag in soup_xsd.find_all(['link:roletype','roletype']):
        for statement_sub_tag in statement_tag:
            if statement_sub_tag.name in ['link:definition','definition']: 
                statement_role_str = re.search('/role/.+$',statement_tag['roleuri'])
                if statement_role_str is not None:
                    statement_str = statement_role_str.group(0)[6:].lower()
                    if statement_str in stock_dict_with_ded:
                        stock_dict_with_ded[statement_str]['statement_name'] = statement_sub_tag.get_text() 

    return stock_dict_with_ded

def do_context_mapping(parsed_xml):

    context_dict = dict()
    context = parsed_xml.xpath("//*[local-name()='context']")


    for element in context:
        contextref_id = element.attrib['id']
        context_dict[contextref_id] = dict()
        for c in element.iter(): 

            context_tag_str = etree.QName(c).localname

            if context_tag_str in ['segment']:
                c_em = c.xpath("*[local-name()='explicitMember']")
                if len(c_em) > 0:
                    context_dict[contextref_id][context_tag_str] = c_em[0].text 
            elif context_tag_str in ['startDate','endDate','instant']:
                context_dict[contextref_id][context_tag_str] = c.text 
        
    return context_dict
    



                  
    




####UTILITY FUNCTIONS

def convert_str_name_tag(str_name):
    return str_name.replace('_',':').lower()

def convert_tag_name_str(tag_name):
    return tag_name.replace(':','_').lower()

def create_dict_if_new_key(s_key,s_dict):
    if s_key not in s_dict:
        s_dict[s_key] = dict()
    return s_dict

def create_soup_object(dirname,filename):
    with open(os.path.join(dirname, filename), "r") as f:
        contents = f.read()
        soup = BeautifulSoup(contents, 'lxml')
    return soup

