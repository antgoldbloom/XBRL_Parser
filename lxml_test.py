

import os
import re 

from lxml import etree
from xml.etree.ElementTree import tostring

CALC_NAMESPACES = {"xlink": "http://www.w3.org/1999/xlink","link":"http://www.xbrl.org/2003/linkbase"}

xbrl_path = '../data/xbrl/' #INCLUDE LAST /
ticker = 'AMZN'
document_end_date = '2020-03-31'
filepath = f"{xbrl_path}{ticker}/{document_end_date}"

filename = 'amzn-20200331_cal.xml'


xml_file = os.path.join(filepath,filename)
parsed_xml = etree.parse(xml_file)

loc_list = parsed_xml.xpath("//*[name()='link:loc']",namespaces=CALC_NAMESPACES)

calc_dict = dict()

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
        if statement_role_str not in calc_dict:
            calc_dict[statement_role_str] = dict()
        calc_dict[statement_role_str][metric] = int(calculation_element[0].xpath('@weight')[0]) 

