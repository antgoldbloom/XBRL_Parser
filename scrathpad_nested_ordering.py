import os
from bs4 import BeautifulSoup, Tag
import re

xbrl_path = '../data/xbrl/AMZN/2020-03-31/'
pre_file = 'amzn-20200331_pre.xml'

def create_soup_object(dirname,filename):
    with open(os.path.join(dirname, filename), "r") as f:
        contents = f.read()
        soup = BeautifulSoup(contents, 'lxml')
    return soup


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

soup_pre = create_soup_object(xbrl_path,pre_file)

stock_dict_with_ded = dict()

#for statement_tag in soup_pre.find_all(['link:presentationlink','presentationlink']):
"""
    link:loc role link:loc xlink:label  
    link:loc xlink:label link:presentationArc xlink:to 
    link:presentationArc xlink:to link:presentationArc xlink:from
    link:presentationArc xlink:from link:loc xlink:label
"""

"""
loc_us-gaap_incomestatementabstract_fb4b13142d9f9f780725bbf517df1f70

    us-gaap_statementtable 1
    us-gaap_costsandexpensesabstract 2
    us-gaap_operatingincomeloss 3
    us-gaap_investmentincomeinterest 4
    us-gaap_interestexpense 5
    us-gaap_othernonoperatingincomeexpense 6
    us-gaap_nonoperatingincomeexpense 7
    us-gaap_incomelossfromcontinuingoperationsbeforeincometaxesminorityinterestandincomelossfromequitymethodinvestments 8
    us-gaap_incometaxexpensebenefit 9
    us-gaap_incomelossfromequitymethodinvestments 10
    us-gaap_netincomeloss 11
    us-gaap_earningspersharebasic 12
    us-gaap_earningspersharediluted 13
    us-gaap_weightedaveragenumberofsharesoutstandingabstract 14
"""

statement_tag = soup_pre.find('link:presentationlink',{'xlink:role': 'http://www.amazon.com/role/ConsolidatedStatementsOfOperations'})

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

print(stock_dict_with_ded)