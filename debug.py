import sys
sys.path.append('sec_xbrl_classes')

import requests
import csv
from datetime import datetime
from time import time
import os 
from utils import setup_logging 

from execute_all_classes import xbrl_to_statement 

from company_statements_xbrl import CompanyStatementsXBRL
from company_statements_json import CompanyStatementsJSON
from company_statements_csv import CompanyStatementCSV
from company_statements_timeseries import CompanyStatementTimeseries
from company_statements_standardize import CompanyStatementStandardize 
from company_stockrow_reconcilation import CompanyStockrowReconcilation 


def get_all_tickers(list_size=500):
    resp = requests.get('https://www.sec.gov/include/ticker.txt')
    ticker_cik_text = resp.content.decode('utf-8')
    ticker_cik_list = list(csv.reader(ticker_cik_text.splitlines(), delimiter='\t'))
    
    ticker_list = []
    for item in ticker_cik_list:
        ticker_list.append(item[0].upper())

    return ticker_list

def fetch_ticker_list(list_name='sample_list'): 
    
    if list_name == 'all_tickers':
       ticker_list = get_all_tickers() 
    elif list_name == 'sp500':
        #ticker_list = ['DD','DXC','ETFC','EMN','ETN','EBAY','ECL','EIX','EW','EA','EMR','ETR','EOG','EFX','EQIX','EQR','ESS','EL','EVRG','ES','RE','EXC','EXPE','EXPD','EXR','XOM','FFIV','FB','FAST','FRT','FDX','FIS','FITB','FE','FISV','FLT','FLIR','FLS','FMC','F','FTNT','FTV','FBHS','FOXA','FOX','BEN','FCX','GPS','GRMN','IT','GD','GE','GIS','GM','GPC','GILD','GL','GPN','GS','GWW','HRB','HAL']
        ticker_list = ['HBI','HOG','HIG','HAS','HCA','PEAK','HP','HSIC','HSY','HES','HPE','HLT','HFC','HOLX','HD','HON','HRL','HST','HPQ','HUM','HBAN','HII','IEX','IDXX','INFO','ITW','ILMN','IR','INTC','ICE','IBM','INCY','IP','IPG','IFF','INTU','ISRG','IVZ','IPGP','IQV','IRM','JKHY','J','JBHT','SJM','JNJ','JCI','JPM','JNPR','KSU','K','KEY','KEYS','KMB','KIM','KMI','KLAC','KSS','KHC','KR','LB','LHX','LH','LRCX','LW','LVS','LEG','LDOS','LEN','LLY','LNC','LIN','LYV','LKQ','LMT','L','LOW','LYB','MTB','M','MRO','MPC','MKTX','MAR','MMC','MLM','MAS','MA','MKC','MXIM','MCD','MCK','MDT','MRK','MET','MTD','MGM','MCHP','MU','MSFT','MAA','MHK','TAP','MDLZ','MNST','MCO','MS','MOS','MSI','MSCI','MYL','NDAQ','NOV','NTAP','NFLX','NWL','NEM','NWSA','NWS','NEE','NLSN','NKE','NI','NBL','JWN','NSC','NTRS','NOC','NLOK','NCLH','NRG','NUE','NVDA','NVR','ORLY','OXY','ODFL','OMC','OKE','ORCL','PCAR','PKG','PH','PAYX','PAYC','PYPL','PNR','PBCT','PEP','PKI','PRGO','PFE','PM','PSX','PNW','PXD','PNC','PPG','PPL','PFG','PG','PGR','PLD','PRU','PEG','PSA','PHM','PVH','QRVO','PWR','QCOM','DGX','RL','RJF','RTN','O','REG','REGN','RF','RSG','RMD','RHI','ROK','ROL','ROP','ROST','RCL','SPGI','CRM','SBAC','SLB','STX','SEE','SRE','NOW','SHW','SPG','SWKS','SLG','SNA','SO','LUV','SWK','SBUX','STT','STE','SYK','SIVB','SYF','SNPS','SYY','TMUS','TROW','TTWO','TPR','TGT','TEL','FTI','TFX','TXN','TXT','TMO','TIF','TJX','TSCO','TDG','TRV','TFC','TWTR','TSN','UDR','ULTA','USB','UAA','UA','UNP','UAL','UNH','UPS','URI','UTX','UHS','UNM','VFC','VLO','VAR','VTR','VRSN','VRSK','VZ','VRTX','V','VNO','VMC','WRB','WAB','WMT','WBA','DIS','WM','WAT','WEC','WFC','WELL','WDC','WU','WRK','WY','WHR','WMB','WLTW','WYNN','XEL','XRX','XLNX','XYL','YUM','ZBRA','ZBH','ZION','ZTS','MMM','ABT','ABBV','ABMD','ACN','ATVI','ADBE','AMD','AAP','AES','AFL','A','APD','AKAM','ALK','ALB','ARE','ALXN','ALGN','ALLE','AGN','ADS','LNT','ALL','GOOG','MO','AMZN','AMCR','AEE','AAL','AEP','AXP','AIG','AMT','AWK','AMP','ABC','AME','AMGN','APH','ADI','ANSS','ANTM','AON','AOS','APA','AIV','AAPL','AMAT','APTV','ADM','ARNC','ANET','AJG','AIZ','ATO','T','ADSK','ADP','AZO','AVB','AVY','BKR','BLL','BAC','BK','BAX','BDX','BBY','BIIB','BLK','BA','BKNG','BWA','BXP','BSX','BMY','AVGO','BR','CHRW','COG','CDNS','CPB','COF','CPRI','CAH','KMX','CCL','CAT','CBOE','CBRE','CDW','CE','CNC','CNP','CTL','CERN','CF','SCHW','CHTR','CVX','CMG','CB','CHD','CI','XEC','CINF','CTAS','CSCO','C','CFG','CTXS','CLX','CME','CMS','KO','CTSH','CL','CMCSA','CMA','CAG','CXO','COP','ED','STZ','COO','GLW','CTVA','COST','COTY','CCI','CPRT','CSX','CMI','CVS','DHI','DHR','DRI','DVA','DE','DAL','XRAY','DVN','FANG','DLR','DFS','DISCA','DISCK','DISH','DG','DLTR','D','DOV','DOW','DTE','DUK','DRE']
        

    elif list_name == 'sample_list':
        ticker_list = ['MSFT','AMZN','NVDA','ORCL','INTC','CRM','TSLA','FB','AAPL','NFLX','ZM','ZEN','ADBE','UBER','LYFT','ZG','WORK','OKTA','BYND','GOOG']
        #ticker_list = ['AAPL','AXP','BA','CAT','CSCO','CVX','DD','DIS','GE','GS','HD','IBM','INTC','JNJ','JPM','KO','MCD','MMM','MRK','MSFT','NKE','PFE','PG','TRV','UNH','UTX','V','VZ']
        #ticker_list = ['WMT','XOM']
    elif list_name == 'debug':
        ticker_list = ['ZM']

    return ticker_list 


data_path="/Users/goldbloom/Dropbox/Side Projects/Edgar/data/"

ticker_list = fetch_ticker_list('all_tickers')
update_only = False 
log_time = datetime.now().strftime('%Y_%m_%d__%H_%M_%S')
bucket_name = 'kaggle_sec_data'

os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="/Users/goldbloom/Dropbox/Side Projects/Edgar/Key/kaggle-playground-0f760ec0ebcd.json"


overall_logger = setup_logging(f"{data_path}/logs/__OVERALL__/",f'{log_time}.log',f'error_{log_time}')

for ticker in ['ZM']: 
    overall_logger.info(f'______{ticker}______')
    start_time = time()
    company_stockrow = CompanyStockrowReconcilation(ticker,data_path,overall_logger)
    overall_logger.info(f"______{time()-start_time}______") 




