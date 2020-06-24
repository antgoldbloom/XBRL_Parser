

import os
import re 

from lxml import etree
from xml.etree.ElementTree import tostring

CALC_NAMESPACES = {"xlink": "http://www.w3.org/1999/xlink","link":"http://www.xbrl.org/2003/linkbase"}
INSTANT_NAMESPACES = {"xbrldi": "http://xbrl.org/2006/xbrldi"} 

xbrl_path = '../data/xbrl/' #INCLUDE LAST /
ticker = 'AMZN'
document_end_date = '2020-03-31'
filepath = f"{xbrl_path}{ticker}/{document_end_date}"

filename = {}
filename['cal'] = 'amzn-20200331_cal.xml'
filename['instance'] = 'amzn-20200331x10q_htm.xml'

xml_file = os.path.join(filepath,filename['instance'])
parsed_xml = etree.parse(xml_file)

xpath_query_list = []
parsed_xml = parsed_xml.getroot()
for key in parsed_xml.nsmap:
    if key not in [None,'dei','iso4217','link','srt','xbrldi','xlink','xsi']:
        xpath_query_list.append(key)

query_str = ('|'.join([f"{x}:*" for x in xpath_query_list]))

ns_dict = parsed_xml.nsmap
del(ns_dict[None])

print(parsed_xml.xpath(f"//{query_str}",namespaces=ns_dict))