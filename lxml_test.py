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

ns_dict = parsed_xml.nsmap
#del(ns_dict[None])

parsed_xml_loc = parsed_xml.xpath(f"//link:loc",namespaces=ns_dict)

lab_dict = {}

for loc in parsed_xml_loc:
    metric_href_elements = loc.xpath('@xlink:href',namespaces=ns_dict)
    for metric_href in metric_href_elements:
        label = loc.xpath('@xlink:label',namespaces=ns_dict)[0]
        metric = metric_href[metric_href.find('#')+1:].lower()
        if metric not in lab_dict:
            lab_dict[metric] = {}
    
        lab_dict[metric]['xlink:label'] = label 

for metric in lab_dict:
    lab_dict[metric]['label'] = {} 
    xlink_label = lab_dict[metric]['xlink:label']
        #print(label)
    label_xml = parsed_xml.xpath(f"//link:label[@xlink:label='lab_{xlink_label[4:]}']",namespaces=ns_dict)
    for element in label_xml:
        label_role = element.get(f"{{{ns_dict['xlink']}}}role")
        label_role = re.search('/role/[A-Za-z]+',label_role).group(0)[6:] 
        lab_dict[metric]['label'][label_role] = element.text

print(lab_dict)

        
print(f"Total: time: {time() - overall_start_time}")