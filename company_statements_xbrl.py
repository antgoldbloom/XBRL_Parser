# %% [markdown]
# Credit to this Github repo: which I cloned and modified for my purpose
# https://github.com/jadchaar/sec-edgar-downloader
#     


SEC_EDGAR_BASE_URL = "https://www.sec.gov/cgi-bin/browse-edgar?"
W3_NAMESPACE = {"w3": "http://www.w3.org/2005/Atom"}


import re
import time
from collections import namedtuple
from datetime import datetime, date
from urllib.parse import urlencode

import requests
from lxml import etree
from bs4 import BeautifulSoup
import csv
import numpy as np

from pathlib import Path

import random
import os

from utils import setup_logging

import requests

import shutil
import zipfile

import logging

import sys

class CompanyStatementsXBRL:
    def __init__(self,ticker,data_path,overall_logger,update_only=True):

        
        overall_start_time = time.time()

        self.ticker = ticker 
        self.log_path = f'{data_path}logs/{ticker}/{overall_logger.name[6:]}/'
        self.xbrl_path = f'{data_path}xbrl/{ticker}/' 
        self.download_count = 0 

        if not update_only:
            shutil.rmtree(f"{self.xbrl_path}", ignore_errors=True, onerror=None)  #remove if exists 

        xbrl_logger = setup_logging(self.log_path,'download_xbrl.log',f'xbrl_{ticker}')
        xbrl_logger.info('______{ticker}_XBRL_download______')

        self.get_filings(xbrl_logger)

        time_taken = f"Total time: {time.time() - overall_start_time}"
        xbrl_logger.info(f"Download count: {self.download_count}")
        xbrl_logger.info(time_taken)



    def get_filings(self,xbrl_logger):
        
        num_filings_to_download = sys.maxsize     # need a large number to denote this
        after_date=None

        for filing_type in ['10-Q','10-K']:
            xbrl_logger.info(f"__{filing_type}__")
            filings_to_fetch = self.get_filing_urls_to_download(filing_type,num_filings_to_download,after_date,xbrl_logger)
            self.download_filings(filing_type, filings_to_fetch,xbrl_logger)


    def list_already_downloaded_statements(self):
        list_already_downloaded = []
        if os.path.exists(self.xbrl_path):
            for already_downloaded in os.listdir(self.xbrl_path):
                list_already_downloaded.append(already_downloaded)

        return list_already_downloaded


    def get_filing_urls_to_download(self,filing_type,num_filings_to_download,after_date, xbrl_logger):
        FilingMetadata = namedtuple("FilingMetadata", ["url_base","xbrl_files","period_end"])
        filings_to_fetch = []
        start = 0
        count = 100

        list_already_downloaded = self.list_already_downloaded_statements()

        while len(filings_to_fetch) < num_filings_to_download:

            qs = self.form_query_string(
                start, count, filing_type, "exclude"
            )
            edgar_search_url = f"{SEC_EDGAR_BASE_URL}{qs}"
            try:
                resp = requests.get(edgar_search_url)
                resp.raise_for_status()
            except:
                xbrl_logger.error("Failed to download {edgar_search_url}")
                return []

            # An HTML page is returned when an invalid ticker is entered
            # Filter out non-XML responses
            if (resp.headers["Content-Type"] != "application/atom+xml"): 
                xbrl_logger.error(f"Ticker {self.ticker} not found (search_query: {edgar_search_url})") 
                return []
            
            # Need xpath capabilities of lxml because some entries are mislabeled as
            # 10-K405, for example, which makes an exact match of filing type infeasible
            #xpath_selector = "//w3:filing-type[not(contains(text(), '/A'))]/.." #excludes amendments
            xpath_selector = f"//w3:filing-type[text() = '{filing_type}']/.."


            filing_entry_elts = self.extract_elements_from_xml(resp.content, xpath_selector)

            # no more filings available
            if not filing_entry_elts:
                break

            for elt in filing_entry_elts:
                # after date constraint needs to be checked if it is passed in
                if after_date is not None:
                    filing_date = elt.findtext("w3:filing-date", namespaces=W3_NAMESPACE)
                    filing_date = filing_date.replace("-", "", 2)
                    if filing_date < after_date:
                        return filings_to_fetch[:num_filings_to_download]

                search_result_url = elt.findtext("w3:filing-href", namespaces=W3_NAMESPACE)

                url_base = '/'.join(search_result_url.split("/")[:-1])

                resp = requests.get(search_result_url)
                soup = BeautifulSoup(resp.content, 'lxml') 
                
                try:

                    period_end=soup.find('div',text='Period of Report').parent.find('div',text=re.compile('[0-9]{4}-[0-9]{2}-[0-9]{2}')).get_text()

                    if period_end not in list_already_downloaded: 

                        xbrl_files = dict()
                        xbrl_files['htm']=soup.find(text=re.compile('XBRL INSTANCE DOCUMENT|EX-101.INS')).parent.parent.find('a',href=re.compile('xml')).get_text()
                        xbrl_files['lab']=soup.find('td',text='EX-101.LAB').parent.find('a',href=re.compile('xml')).get_text()
                        xbrl_files['pre']=soup.find('td',text='EX-101.PRE').parent.find('a',href=re.compile('xml')).get_text()
                        xbrl_files['xsd']=soup.find('td',text='EX-101.SCH').parent.find('a',href=re.compile('xsd')).get_text()
                        xbrl_files['cal']=soup.find('td',text='EX-101.CAL').parent.find('a',href=re.compile('xml')).get_text()

                
                        filings_to_fetch.append(
                            FilingMetadata(url_base=url_base,xbrl_files=xbrl_files,period_end=period_end)
                        )
                        xbrl_logger.info(f"{period_end}: XBRL found")
                    else:
                        xbrl_logger.info(f"{period_end}: XBRL already exists so not downloaded")
                except:
                    xbrl_logger.warning(f"{search_result_url}: no XBRL found")

            start += count

        
        return filings_to_fetch[:num_filings_to_download]



    def form_query_string(self,start, count, filing_type, ownership="exclude"):
        query_params = {
            "action": "getcompany",
            "owner": ownership,
            "start": start,
            "count": count,
            "CIK": self.ticker, 
            "type": filing_type,
            "dateb": None,
            "output": "atom",
        }
        return urlencode(query_params)



    def extract_elements_from_xml(self,xml_byte_object, xpath_selector):
        xml_root = etree.fromstring(xml_byte_object)
        return xml_root.xpath(xpath_selector, namespaces=W3_NAMESPACE)


                


    def validate_date_format(self,date_str):
        try:
            datetime.strptime(date_str, "%Y%m%d")
        except ValueError:
            raise ValueError(
                "Incorrect date format. Please enter a date string of the form YYYYMMDD."
            )



    def download_filings(self, filing_type, filings_to_fetch,xbrl_logger):
        for filing in filings_to_fetch:

            for x_file in filing.xbrl_files:
                try:
                    resp = requests.get(f"{filing.url_base}/{filing.xbrl_files[x_file]}")
                    resp.raise_for_status()
                except:
                    xbrl_logger.error(f">>Failed to download {filing.url_base}/{filing.xbrl_files[x_file]}")
                else:
                    save_path = Path(self.xbrl_path).joinpath(
                        filing.period_end,filing.xbrl_files[x_file] 
                    )
                    # Create all parent directories as needed
                    save_path.parent.mkdir(parents=True, exist_ok=True)

                    with open(save_path, "w", encoding="utf-8") as f:
                        f.write(resp.text)
                    # SEC limits users to 10 downloads per second
                    # Sleep >0.10s between each download to prevent rate-limiting
                    # https://github.com/jadchaar/sec-edgar-downloader/issues/24
                    xbrl_logger.info(f'>>{filing.period_end}: {x_file} downloaded')
                    self.download_count += 1

                    time.sleep(0.15)

                    



# %% [code]

