

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

context_dict = dict()
context = parsed_xml.xpath("//*[local-name()='context']")

for element in context:
    contextref_id = element.attrib['id']
    context_dict[contextref_id] = dict()
    for c in element.iter(): 
    
        context_tag_str = etree.QName(c).localname
        if context_tag_str in ['segment']:
            print(contextref_id)
            c_em = c.xpath("*[name()='xbrldi:explicitMember']",namespaces=INSTANT_NAMESPACES)
            if len(c_em) > 0:
                context_dict[contextref_id][context_tag_str] = c_em[0].text 
        elif context_tag_str in ['startDate','endDate','instant']:
            context_dict[contextref_id][context_tag_str] = c.text 

        
print(context_dict)

""" 
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

 """