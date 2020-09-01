import time
import os
from google.cloud import pubsub_v1

import requests
import csv

import datetime
import numpy as np
from yahoo_earnings_calendar import YahooEarningsCalendar


project_id = "kaggle-playground-170215"

os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="/Users/goldbloom/Dropbox/Side Projects/Edgar/Key/kaggle-playground-0f760ec0ebcd.json"

def fetch_ticker_list(list_name='sample_list'): 
    
    if list_name == 'all_tickers':
       ticker_list = get_all_tickers() 
    elif list_name == 'earnings_today':
       ticker_list = get_todays_earnings()
    elif list_name == 'sp500':
        ticker_list = ['MMM',	'ABT',	'ABBV',	'ABMD',	'ACN',	'ATVI',	'ADBE',	'AMD',	'AAP',	'AES',	'AFL',	'A',	'APD',	'AKAM',	'ALK',	'ALB',	'ARE',	'ALXN',	'ALGN',	'ALLE',	'LNT',	'ALL',	'GOOGL',	'GOOG',	'MO',	'AMZN',	'AMCR',	'AEE',	'AAL',	'AEP',	'AXP',	'AIG',	'AMT',	'AWK',	'AMP',	'ABC',	'AME',	'AMGN',	'APH',	'ADI',	'ANSS',	'ANTM',	'AON',	'AOS',	'APA',	'AIV',	'AAPL',	'AMAT',	'APTV',	'ADM',	'ANET',	'AJG',	'AIZ',	'T',	'ATO',	'ADSK',	'ADP',	'AZO',	'AVB',	'AVY',	'BKR',	'BLL',	'BAC',	'BK',	'BAX',	'BDX',	'BRK.B',	'BBY',	'BIO',	'BIIB',	'BLK',	'BA',	'BKNG',	'BWA',	'BXP',	'BSX',	'BMY',	'AVGO',	'BR',	'BF.B',	'CHRW',	'COG',	'CDNS',	'CPB',	'COF',	'CAH',	'KMX',	'CCL',	'CARR',	'CAT',	'CBOE',	'CBRE',	'CDW',	'CE',	'CNC',	'CNP',	'CTL',	'CERN',	'CF',	'SCHW',	'CHTR',	'CVX',	'CMG',	'CB',	'CHD',	'CI',	'CINF',	'CTAS',	'CSCO',	'C',	'CFG',	'CTXS',	'CLX',	'CME',	'CMS',	'KO',	'CTSH',	'CL',	'CMCSA',	'CMA',	'CAG',	'CXO',	'COP',	'ED',	'STZ',	'COO',	'CPRT',	'GLW',	'CTVA',	'COST',	'COTY',	'CCI',	'CSX',	'CMI',	'CVS',	'DHI',	'DHR',	'DRI',	'DVA',	'DE',	'DAL',	'XRAY',	'DVN',	'DXCM',	'FANG',	'DLR',	'DFS',	'DISCA',	'DISCK',	'DISH',	'DG',	'DLTR',	'D',	'DPZ',	'DOV',	'DOW',	'DTE',	'DUK',	'DRE',	'DD',	'DXC',	'ETFC',	'EMN',	'ETN',	'EBAY',	'ECL',	'EIX',	'EW',	'EA',	'EMR',	'ETR',	'EOG',	'EFX',	'EQIX',	'EQR',	'ESS',	'EL',	'EVRG',	'ES',	'RE',	'EXC',	'EXPE',	'EXPD',	'EXR',	'XOM',	'FFIV',	'FB',	'FAST',	'FRT',	'FDX',	'FIS',	'FITB',	'FE',	'FRC',	'FISV',	'FLT',	'FLIR',	'FLS',	'FMC',	'F',	'FTNT',	'FTV',	'FBHS',	'FOXA',	'FOX',	'BEN',	'FCX',	'GPS',	'GRMN',	'IT',	'GD',	'GE',	'GIS',	'GM',	'GPC',	'GILD',	'GL',	'GPN',	'GS',	'GWW',	'HRB',	'HAL',	'HBI',	'HIG',	'HAS',	'HCA',	'PEAK',	'HSIC',	'HSY',	'HES',	'HPE',	'HLT',	'HFC',	'HOLX',	'HD',	'HON',	'HRL',	'HST',	'HWM',	'HPQ',	'HUM',	'HBAN',	'HII',	'IEX',	'IDXX',	'INFO',	'ITW',	'ILMN',	'INCY',	'IR',	'INTC',	'ICE',	'IBM',	'IP',	'IPG',	'IFF',	'INTU',	'ISRG',	'IVZ',	'IPGP',	'IQV',	'IRM',	'JKHY',	'J',	'JBHT',	'SJM',	'JNJ',	'JCI',	'JPM',	'JNPR',	'KSU',	'K',	'KEY',	'KEYS',	'KMB',	'KIM',	'KMI',	'KLAC',	'KSS',	'KHC',	'KR',	'LB',	'LHX',	'LH',	'LRCX',	'LW',	'LVS',	'LEG',	'LDOS',	'LEN',	'LLY',	'LNC',	'LIN',	'LYV',	'LKQ',	'LMT',	'L',	'LOW',	'LYB',	'MTB',	'MRO',	'MPC',	'MKTX',	'MAR',	'MMC',	'MLM',	'MAS',	'MA',	'MKC',	'MXIM',	'MCD',	'MCK',	'MDT',	'MRK',	'MET',	'MTD',	'MGM',	'MCHP',	'MU',	'MSFT',	'MAA',	'MHK',	'TAP',	'MDLZ',	'MNST',	'MCO',	'MS',	'MOS',	'MSI',	'MSCI',	'MYL',	'NDAQ',	'NOV',	'NTAP',	'NFLX',	'NWL',	'NEM',	'NWSA',	'NWS',	'NEE',	'NLSN',	'NKE',	'NI',	'NBL',	'NSC',	'NTRS',	'NOC',	'NLOK',	'NCLH',	'NRG',	'NUE',	'NVDA',	'NVR',	'ORLY',	'OXY',	'ODFL',	'OMC',	'OKE',	'ORCL',	'OTIS',	'PCAR',	'PKG',	'PH',	'PAYX',	'PAYC',	'PYPL',	'PNR',	'PBCT',	'PEP',	'PKI',	'PRGO',	'PFE',	'PM',	'PSX',	'PNW',	'PXD',	'PNC',	'PPG',	'PPL',	'PFG',	'PG',	'PGR',	'PLD',	'PRU',	'PEG',	'PSA',	'PHM',	'PVH',	'QRVO',	'PWR',	'QCOM',	'DGX',	'RL',	'RJF',	'RTX',	'O',	'REG',	'REGN',	'RF',	'RSG',	'RMD',	'RHI',	'ROK',	'ROL',	'ROP',	'ROST',	'RCL',	'SPGI',	'CRM',	'SBAC',	'SLB',	'STX',	'SEE',	'SRE',	'NOW',	'SHW',	'SPG',	'SWKS',	'SLG',	'SNA',	'SO',	'LUV',	'SWK',	'SBUX',	'STT',	'STE',	'SYK',	'SIVB',	'SYF',	'SNPS',	'SYY',	'TMUS',	'TROW',	'TTWO',	'TPR',	'TGT',	'TEL',	'FTI',	'TDY',	'TFX',	'TXN',	'TXT',	'TMO',	'TIF',	'TJX',	'TSCO',	'TT',	'TDG',	'TRV',	'TFC',	'TWTR',	'TYL',	'TSN',	'UDR',	'ULTA',	'USB',	'UAA',	'UA',	'UNP',	'UAL',	'UNH',	'UPS',	'URI',	'UHS',	'UNM',	'VFC',	'VLO',	'VAR',	'VTR',	'VRSN',	'VRSK',	'VZ',	'VRTX',	'VIAC',	'V',	'VNO',	'VMC',	'WRB',	'WAB',	'WMT',	'WBA',	'DIS',	'WM',	'WAT',	'WEC',	'WFC',	'WELL',	'WST',	'WDC',	'WU',	'WRK',	'WY',	'WHR',	'WMB',	'WLTW',	'WYNN',	'XEL',	'XRX',	'XLNX',	'XYL',	'YUM',	'ZBRA',	'ZBH',	'ZION',	'ZTS']
    elif list_name == 'sample_list':
        ticker_list = ['MSFT','AMZN','NVDA','ORCL','INTC','CRM','TSLA','FB','AAPL','NFLX','ZM','ZEN','ADBE','UBER','LYFT','ZG','WORK','OKTA','BYND','GOOG']
        #ticker_list = ['AAPL','AXP','BA','CAT','CSCO','CVX','DD','DIS','GE','GS','HD','IBM','INTC','JNJ','JPM','KO','MCD','MMM','MRK','MSFT','NKE','PFE','PG','TRV','UNH','UTX','V','VZ']
        #ticker_list = ['WMT','XOM']
    elif list_name == 'debug':
        ticker_list = ['HYAS']

    return ticker_list 

                    
def get_all_tickers():
    resp = requests.get('https://www.sec.gov/include/ticker.txt')
    ticker_cik_text = resp.content.decode('utf-8')
    ticker_cik_list = list(csv.reader(ticker_cik_text.splitlines(), delimiter='\t'))
    
    ticker_list = []
    for item in ticker_cik_list:
        ticker_list.append(item[0].upper())

    return ticker_list

def get_todays_earnings():
    today = datetime.date.today()
    date_from = datetime.datetime.combine(today, datetime.datetime.min.time())
    date_to = datetime.datetime.combine(today, datetime.datetime.max.time())

    yec = YahooEarningsCalendar()
    earnings_list = yec.earnings_between(date_from, date_to)

    ticker_list = []
    for stock in earnings_list:
        ticker_list.append(stock['ticker'])
    
    return np.unique(ticker_list)


for ticker in fetch_ticker_list('all_tickers'): 

    print(ticker)
    update_only = "t" 

    publisher = pubsub_v1.PublisherClient()
    topic_name = 'projects/{project_id}/topics/{topic}'.format(project_id=project_id,topic='download_xbrl_pubsub_topic')
    publisher.publish(topic_name, data=f"{ticker} published".encode('utf-8'), ticker=ticker, update_only=update_only)