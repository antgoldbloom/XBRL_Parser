from parse_XBRL_files_utility_script import create_stock_dict
import json
from time import time

start_time = time()

ticker = 'ZG'
xbrl_path = '../data/xbrl/sec_filings/' #INCLUDE LAST /
json_path  = '../data/json/'  #INCLUDE LAST /
stock_dict = create_stock_dict(ticker,xbrl_path)

with open(f"{json_path}{ticker}.json", 'w') as stock_json:
    json.dump(stock_dict, stock_json)

print(time() - start_time)