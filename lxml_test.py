from time import time

import os
import re 

from lxml import etree
from xml.etree.ElementTree import tostring

overall_start_time = time()

CALC_NAMESPACES = {"xlink": "http://www.w3.org/1999/xlink","link":"http://www.xbrl.org/2003/linkbase"}
INSTANT_NAMESPACES = {"xbrldi": "http://xbrl.org/2006/xbrldi"} 

xbrl_path = '../data/xbrl/' #INCLUDE LAST /
ticker = 'AMZN'
document_end_date = '2020-03-31'
filepath = f"{xbrl_path}{ticker}/{document_end_date}"

filename = {}
filename['lab'] = 'amzn-20200331_lab.xml'
filename['instance'] = 'amzn-20200331x10q_htm.xml'

xml_file = os.path.join(filepath,filename['lab'])
parsed_xml = etree.parse(xml_file)

xpath_query_list = []
parsed_xml = parsed_xml.getroot()

LABEL_NAMESPACE = {'link': 'http://www.xbrl.org/2003/linkbase', 'xsi': 'http://www.w3.org/2001/XMLSchema-instance', 'xlink': 'http://www.w3.org/1999/xlink'}
#del(LABEL_NAMESPACE[None])

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


""" 
for metric in lab_dict:
    lab_dict[metric]['label'] = {} 
    xlink_label = lab_dict[metric]['xlink:label']
        #print(label)
    label_xml = parsed_xml.xpath(f"//link:label[@xlink:label='lab_{xlink_label[4:]}']",namespaces=LABEL_NAMESPACE)
    for element in label_xml:
        label_role = element.get(f"{{{LABEL_NAMESPACE['xlink']}}}role")
        label_role = re.search('/role/[A-Za-z]+',label_role).group(0)[6:] 
        lab_dict[metric]['label'][label_role] = element.text

 """
        
print(lab_dict)

print(f"Total: time: {time() - overall_start_time}")