
import os
import pandas as pd
import numpy as np
import re 
import shutil
from pathlib import Path
from datetime import datetime
from time import time


from collections import OrderedDict 

from utils import setup_logging,upload_statement_files, download_statement_files, delete_statement_files,upload_log_file

import string
from sklearn.metrics.pairwise import cosine_similarity
from sklearn.feature_extraction.text import CountVectorizer

class CompanyStatementTimeseries:


    def __init__(self,ticker,data_path,overall_logger,bucket_name,update_only=True,statement=None):

        overall_start_time = time()
        #initialize key variables
        self.ticker = ticker 
        self.data_root_path = f"{data_path}data/{ticker}/"
        self.csv_path = f"{self.data_root_path}csv/"
        self.log_path = f"{data_path}logs/{ticker}/{overall_logger.name[6:]}/"
        self.timeseries_path = f"{self.data_root_path}timeseries/"
        self.freq_list = ['instant','qtd','6mtd','9mtd','ytd']
        
        #configure logging
        timeseries_logger = setup_logging(self.log_path,'timeseries.log',f'timeseries_{ticker}')
        log_time = datetime.now().strftime('%Y_%m_%d__%H_%M_%S')
        timeseries_logger.info(f'_____{ticker}_TIMESERIES___{log_time}_____')

        shutil.rmtree(f"{self.data_root_path}", ignore_errors=True, onerror=None)  #remove if exists 

        if update_only:
            download_statement_files(bucket_name, data_path, "timeseries",ticker,timeseries_logger)
        else:
            delete_statement_files(bucket_name, data_path, "timeseries",ticker,timeseries_logger)

        download_statement_files(bucket_name, data_path, "csv",ticker,timeseries_logger)        

        self.latest_statement_date_type()

        Path(f"{self.timeseries_path}").mkdir(parents=True, exist_ok=True)

        
        if statement is None: #loop through each statement
            for dirname, _, filenames in os.walk(f"{self.csv_path}/{ self.latest_statement_date_type}"):
                for statement in filenames:
                    timeseries_logger.info(f"__{statement}__")
                    self.create_statement_time_series(statement,timeseries_logger,overall_logger)


            self.statement_count = 0
            for dirname, _, filenames in os.walk(f"{self.csv_path}/{self.latest_statement_date_type}"):
                for filename in filenames:
                    if filename[-4:] == '.csv': 
                        self.statement_count += 1
        else: #for debugging purposes: only look at statement with issue
            timeseries_logger.info(f"__{statement}__")
            self.create_statement_time_series(statement,timeseries_logger,overall_logger)
            self.statement_count = 1


        timeseries_logger.info(f"Statement count: {self.statement_count}")
        time_taken = f"Total time: {time() - overall_start_time}"
        timeseries_logger.info(time_taken)

        upload_log_file(bucket_name, self.log_path,ticker,'timeseries.log')


    def create_statement_time_series(self,statement,timeseries_logger,overall_logger):
        statement_dict = self.load_statements_into_dict(statement,timeseries_logger) 
        list_statement_dates = sorted(list(statement_dict.keys()),reverse=True)

        statement_folder, statement_name = self.statement_file_path_and_name(statement)
        statement_full_path = os.path.join(statement_folder,statement_name)
        needs_update = True 
        if os.path.exists(statement_full_path):
            timeseries_df_current = pd.read_csv(statement_full_path,index_col=[0,1])
            latest_date_from_csv_statement = max(self.date_columns_from_statement(statement_dict[list_statement_dates[0]].columns))
            if latest_date_from_csv_statement == timeseries_df_current.columns.max(): 
                needs_update = False
                timeseries_logger.info(f"Already have latest version of {statement} for {self.ticker}")
            else: 
                os.remove(statement_full_path)

        if needs_update == True:
            timeseries_df = self.populate_timeseries(statement,statement_dict,list_statement_dates,timeseries_logger,overall_logger) 
            timeseries_df = self.add_labels(timeseries_df,statement_dict,list_statement_dates)
            self.save_file(statement,timeseries_df,timeseries_logger)

    def populate_timeseries(self,statement,statement_dict,list_statement_dates,timeseries_logger,overall_logger):

        timeseries_df = self.populate_timeseries_df(statement_dict,list_statement_dates,timeseries_logger,overall_logger) 
        timeseries_df = self.clean_up_timeseries_df(statement_dict[list_statement_dates[0]],timeseries_df,timeseries_logger)
        self.check_dataframe(timeseries_df,timeseries_logger)
        return timeseries_df


    def latest_statement_date_type(self):
        ded_dict = {}
        for dirname in os.listdir(f"{self.csv_path}"):
            ded_dict[dirname[:10]] = dirname 
        
        self.latest_statement_date_type = ded_dict[max(ded_dict.keys())]


    def load_statements_into_dict(self,statement,timeseries_logger):

        statement_dict = {}
        statement_dict[self.latest_statement_date_type] = pd.read_csv(os.path.join(f"{self.csv_path}{self.latest_statement_date_type}",statement),index_col=[0,1])

        missing_list = []
        low_overlap_list = [] 
        for dirname, _, filenames in os.walk(f"{self.csv_path}"):
            missing_list.append(dirname)
            max_overlap = 0
            for filename in filenames:
                if (filename[-4:] == '.csv'): 

                    csim = self.calculate_csim([statement, filename])
                    if csim == 1:
                        most_likely_statement_match = filename 
                        max_overlap = 1
                        break #if filenames map perfectly don't even bother looking for column overlap
                    elif csim > 0.3:
        
                        comp_df = pd.read_csv(os.path.join(dirname,filename),index_col=[0,1])
                        overlap_percentage = len(set(statement_dict[self.latest_statement_date_type].index.get_level_values(0)).intersection(comp_df.index.get_level_values(0)))/len(statement_dict[self.latest_statement_date_type])  
                    
                        if overlap_percentage > max_overlap:
                            max_overlap = overlap_percentage
                            most_likely_statement_match = filename

            if max_overlap > 0:
                full_path = os.path.join(dirname,most_likely_statement_match)
                ds = re.search('[0-9]{4}-[0-9]{2}-[0-9]{2} \(10-[Q|K]\)',full_path)[0]
                statement_dict[ds] = pd.read_csv(full_path,index_col=[0,1])
                missing_list.remove(dirname)


                if max_overlap < 0.3:
                    low_overlap_list.append(f"{dirname[dirname.rfind('/')+1:]} ({round(max_overlap,2)})") 

        missing_list = sorted(np.unique(missing_list),reverse=True)
        self.log_missing(missing_list,sorted(low_overlap_list,reverse=True),statement,self.csv_path, timeseries_logger)

        return statement_dict

    def clean_string(self,text):
        stopwords_file = open("mapping/english", "r")
        stopwords = stopwords_file.readlines()

        text = text[:-4] 
        text = ''.join([word for word in text if word not in string.punctuation])
        text = text.lower()
        text = ' '.join([word for word in text.split() if word not in stopwords])
        return text

    def cosine_sim_vectors(self,vec1,vec2):
        vec1 = vec1.reshape(1,-1)
        vec2 = vec2.reshape(1,-1)
        return cosine_similarity(vec1,vec2)[0][0]

    def calculate_csim(self,statement_names):
        cleaned_statement_names = list(map(self.clean_string,statement_names))
        vectorizer = CountVectorizer().fit_transform(cleaned_statement_names)
        statement_name_vectors = vectorizer.toarray()
        csim = self.cosine_sim_vectors(statement_name_vectors[0],statement_name_vectors[1]) 
        return csim

    def log_missing(self,missing_list,low_overlap_list,statement,csv_path,timeseries_logger):
            
        missing_list = list(missing_list)
        missing_list.remove(f"{csv_path}")

        if len(missing_list) > 0:
            timeseries_logger.warning(f"{statement} equivalent not found in: {', '.join([dirname[dirname.rfind('/')+1:] for dirname in missing_list])}")

        if len(low_overlap_list) > 0:
            timeseries_logger.warning(f"{statement} low overlap found in: {', '.join([lo_item for lo_item in low_overlap_list])}")


    def date_columns_from_statement(self,columns):
        return [date_col for date_col in columns if re.match('[0-9]{4}-[0-9]{2}-[0-9]{2}',date_col)]

    def populate_timeseries_df(self,statement_dict,list_statement_dates,timeseries_logger,overall_logger):

        list_10k = []
        needs_adjustment = {}

        for ds in list_statement_dates:
            
            date_cols = self.date_columns_from_statement(statement_dict[ds].columns) 
            tmp_df = statement_dict[ds][date_cols]

            metric_period_dict = {}

            for ix in tmp_df.index:
                if ix[0] not in metric_period_dict:
                    metric_period_dict[ix[0]] = []

                metric_period_dict[ix[0]].append(ix[1])


            index_to_keep_list = []
            for xbrl_tag in metric_period_dict:
                for freq in self.freq_list:
                    if freq in metric_period_dict[xbrl_tag]:
                        index_to_keep_list.append((xbrl_tag,freq))
                        if freq in ['6mtd','9mtd','ytd']:
                            if xbrl_tag not in needs_adjustment:
                                needs_adjustment[xbrl_tag] = {}                             
                            date_col = ds[:10]
                            needs_adjustment[xbrl_tag][date_col] = freq
                        break

            tmp_df = tmp_df.loc[index_to_keep_list,:]
            tmp_df.index = tmp_df.index.droplevel(1) #remove period_type

            if ds == list_statement_dates[0]: #initialize the dataframe
                timeseries_df = tmp_df
            else:
                new_columns = tmp_df.columns.difference(timeseries_df.columns)
                timeseries_df = timeseries_df.merge(tmp_df[new_columns],left_index=True,right_index=True,how='outer')
                timeseries_df.loc[timeseries_df.index.isin(tmp_df.index),timeseries_df.columns.isin(tmp_df)] = tmp_df

        timeseries_df = self.adjust_for_tag_changes_and_period_types(needs_adjustment,statement_dict,timeseries_df,timeseries_logger,overall_logger)
        

        return timeseries_df


    def apply_tag_map_to_needs_adjustment_dict(self,needs_adjustment, tag_map):
        tag_map_needs_adjustment_intersection = set(tag_map.keys()).intersection(needs_adjustment.keys())

        for matched_xbrl_tag in tag_map_needs_adjustment_intersection:
            for xbrl_tag in tag_map[matched_xbrl_tag]:
                if (xbrl_tag not in needs_adjustment): 
                   needs_adjustment[xbrl_tag] =  needs_adjustment[matched_xbrl_tag]
                else:
                    ##UP TO NEEDS ADJUSTMENT CAN CHANGE. SO LOOKs LIKE IT's CHANGING, which is causiing an issue with the statement. Is that OK
                    needs_adjustment[xbrl_tag].update(needs_adjustment[matched_xbrl_tag])
                needs_adjustment[xbrl_tag] = OrderedDict(sorted(needs_adjustment[xbrl_tag].items(),reverse=True))
            del(needs_adjustment[matched_xbrl_tag])
        return needs_adjustment

    def period_type_to_months(self,period_type):
        if period_type == 'ytd':
            return 12
        elif period_type == '9mtd':
            return 9
        elif period_type == '6mtd':
            return 6
        elif period_type == 'qtd':
            return 3
        else:
            return None
    
    def adjust_to_quarterly(self,needs_adjustment, tag_map, timeseries_df,timeseries_logger,overall_logger):
        needs_adjustment = self.apply_tag_map_to_needs_adjustment_dict(needs_adjustment, tag_map)

        for xbrl_tag in needs_adjustment:
            for date_col in needs_adjustment[xbrl_tag]:
                months_in_metric = self.period_type_to_months(needs_adjustment[xbrl_tag][date_col])
                if date_col in timeseries_df.columns:
                    month_index = timeseries_df.columns.get_loc(date_col)
                    tag_index = timeseries_df.index.get_loc(xbrl_tag)

                    i = 1
                    while (months_in_metric > 3):

                        if month_index +i < len(timeseries_df.columns):
                            if timeseries_df.columns[month_index+i] in needs_adjustment[xbrl_tag]:
                                months_in_metric = months_in_metric - self.period_type_to_months(needs_adjustment[xbrl_tag][timeseries_df.columns[month_index+i]])
                            else:
                                months_in_metric = months_in_metric - 3

                            if months_in_metric >= 3:
                                timeseries_df.iloc[tag_index,month_index] = timeseries_df.iloc[tag_index,month_index] - timeseries_df.iloc[tag_index,month_index+i]
                            else:
                                timeseries_logger.error(f"Error adjusting period type for {xbrl_tag} on {date_col}") 
                            i += 1
                        else: #not enough history in time series to adjust
                            timeseries_df.iloc[tag_index,month_index] = None
                            break


        return timeseries_df

    def adjust_for_tag_changes_and_period_types(self,needs_adjustment,statement_dict,timeseries_df,timeseries_logger,overall_logger): 

        tag_map = {} 
        
        tags_from_latest_statement = timeseries_df[(timeseries_df.isna().any(axis=1) & timeseries_df[timeseries_df.columns.max()].notna())]
        for metric in tags_from_latest_statement.index:
            for metric_match in timeseries_df[~timeseries_df.index.isin(tags_from_latest_statement.index)].index:

                non_nan_columns = timeseries_df.loc[[metric,metric_match],:].notna().all(axis=0)
                agreement_series = timeseries_df.loc[metric,non_nan_columns]==timeseries_df.loc[metric_match,non_nan_columns]

                if (agreement_series.all() == True) and len(agreement_series > 1):
                        timeseries_df.loc[metric,timeseries_df.columns[timeseries_df[timeseries_df.index == metric].isna().iloc[0].to_list()]] = timeseries_df.loc[metric_match,timeseries_df.columns[timeseries_df[timeseries_df.index == metric].isna().iloc[0].to_list()]] 
                        if metric_match not in tag_map:
                            tag_map[metric_match] = [] 
                        tag_map[metric_match].append(metric) 

        for metric in np.unique(tag_map.keys()):
            timeseries_df = timeseries_df.drop(metric,axis=0)

        #timeseries_df.columns = pd.DatetimeIndex(timeseries_df.columns) 
        timeseries_df = timeseries_df.sort_index(axis=1,ascending=False)

        if len(needs_adjustment) > 0:
            timeseries_df = self.adjust_to_quarterly(needs_adjustment, tag_map, timeseries_df,timeseries_logger,overall_logger)

        return timeseries_df




    def clean_up_timeseries_df(self,latest_statement_df,timeseries_df,timeseries_logger):

        #latest_statement_df =  statement_dict[list_statement_dates[0]]
        #latest_statement_df.index = latest_statement_df.index.get_level_values(0)  

        latest_filing_mask = timeseries_df.index.isin(latest_statement_df.index.get_level_values(0))


        #reordering according to the latest statement
        timeseries_df_tmp = timeseries_df.loc[latest_filing_mask,:]
        timeseries_df_tmp = timeseries_df_tmp.reindex(latest_statement_df.index.get_level_values(0))
        #moving rows that aren't in the latest statemeent to the bottom
        timeseries_df_tmp = timeseries_df_tmp.append(timeseries_df.loc[~latest_filing_mask,:])

        #remove columns with all NA
        timeseries_df_tmp = timeseries_df_tmp.loc[~timeseries_df_tmp.isna().all(axis=1),:] 

        timeseries_df = timeseries_df_tmp.drop_duplicates()

        return timeseries_df

    def count_sequential_quarters(self,quarter_list,log_missing=False,timeseries_logger=None):
        sequential_quarters = 0

        missing_quarters = []
        for i in range(1,len(quarter_list)):
            diff = datetime.strptime(quarter_list[i-1],'%Y-%m-%d') - datetime.strptime(quarter_list[i],'%Y-%m-%d')

            if diff.days > 85 and diff.days < 103: #most quarters are separated by between 90 and 98 days
                sequential_quarters +=1
            else:
                if log_missing:
                    missing_quarters.append(f"{quarter_list[i-1]} and {quarter_list[i]}")
                else:
                    break   

        if len(missing_quarters) > 0: 
            timeseries_logger.warning(f"Missing quarter between: {', '.join(missing_quarters)}") 

        return sequential_quarters 

    def check_dataframe(self,timeseries_df,timeseries_logger):


        expected_columns = round((datetime.strptime(timeseries_df.columns.max(), "%Y-%m-%d")-datetime.strptime(timeseries_df.columns.min(), "%Y-%m-%d")).days/(365/4))+1
        actual_columns = len(timeseries_df.columns)
        if (expected_columns - actual_columns) >= 0:  
            message = f'Discontinuities estimate: {expected_columns-actual_columns}'

        sequential_quarters = self.count_sequential_quarters(timeseries_df.columns, True, timeseries_logger)
        timeseries_logger.info(f"Estimate of sequential quarters: {sequential_quarters}")

        timeseries_logger.info(f"Number of periods: {actual_columns}") 

        timeseries_logger.info(f"Number of metrics: {len(timeseries_df)}") 

        na_percentage = round(100*(len(timeseries_df)-timeseries_df.count()).sum()/(len(timeseries_df)*len(timeseries_df.columns)),2)
        timeseries_logger.info(f"NA percentage:  {na_percentage}")

    def statement_file_path_and_name(self,statement,timeseries_logger=None):
        dash_list = [m.start() for m in re.finditer('-', statement)]
        if len(dash_list) >= 2: 
            statement_folder = f'Raw {statement[dash_list[0]+2:dash_list[1]-1]}s'
            statement_name = f"{self.ticker} {statement[dash_list[1]+2:].strip()}"
        else:
            statement_name = f"{self.ticker} {statement.strip()}"
            statement_folder = 'Other'
            if timeseries_logger is not None:
                timeseries_logger(f'Statement type could not be infered for {statement}')

        return [f"{self.timeseries_path}{statement_folder}/",statement_name]

    def save_file(self,statement,timeseries_df,timeseries_logger):

        statement_folder,statement_name = self.statement_file_path_and_name(statement,timeseries_logger)
        Path(statement_folder).mkdir(parents=True, exist_ok=True)

        timeseries_df.to_csv(f"{statement_folder}{statement_name}") 


        
    def add_labels(self,timeseries_df,statement_dict,list_statement_dates):

        timeseries_df = self.add_all_labels(timeseries_df,statement_dict,list_statement_dates)
        timeseries_df = self.consolidate_into_single_label(timeseries_df)

        segment_index_list = [segment for segment in timeseries_df.index if '___' in segment]
        for segment_xbrl_tag in segment_index_list:
            parent_segment_xbrl_tag = segment_xbrl_tag[:segment_xbrl_tag.find('___')] 
            segment_label = timeseries_df.loc[segment_xbrl_tag,'label']  
            if (parent_segment_xbrl_tag in timeseries_df.index): 
                parent_segment_label = timeseries_df.loc[parent_segment_xbrl_tag,'label']  
                if (segment_label not in [None,np.nan]) and (parent_segment_label not in [None,np.nan]): 
                    segment_label = segment_label.replace('[Member]','').strip()
                    timeseries_df.loc[segment_xbrl_tag,'label'] = parent_segment_label + ' (' + segment_label + ')'  



        timeseries_df['xbrl_tag'] = timeseries_df.index 
        timeseries_df = timeseries_df.set_index(['label','xbrl_tag']) 
        


        return timeseries_df 

    def add_all_labels(self,timeseries_df,statement_dict,list_statement_dates):

        for ds in list_statement_dates:

            tmp_df = statement_dict[ds]   
            tmp_df.index = tmp_df.index.droplevel(1) #remove period_type
            label_list = [l for l in tmp_df.columns if 'label' in l .lower()]
            tmp_df = tmp_df[label_list ]
            tmp_df = tmp_df.drop_duplicates()

            for label in label_list:
                if label not in timeseries_df.columns:
                    timeseries_df[label] = None

            if ds ==list_statement_dates[0]:
                timeseries_df.loc[ tmp_df[tmp_df.index.isin(timeseries_df.index)].index,label_list] = tmp_df.loc[:,label_list]
            else:
                labels_to_find_index = tmp_df[tmp_df.index.isin(labels_to_find_list)].index
                timeseries_df.loc[labels_to_find_index,label_list] = tmp_df.loc[labels_to_find_index,label_list] 

            label_list = [l for l in timeseries_df.columns if 'label' in l .lower()] 

            if timeseries_df[label_list].notna().any(axis=1).all() == False: #if any labels are still NA
                labels_to_find_list = timeseries_df[timeseries_df[label_list].notna().any(axis=1) == False].index
            else:
                break

        return timeseries_df


    def consolidate_into_single_label(self,timeseries_df):
        if 'label' not in timeseries_df.columns:
            timeseries_df['label'] = None

        label_list = [l for l in timeseries_df.columns if 'label' in l .lower()] 

        for label in label_list:
            timeseries_df.loc[timeseries_df['label'].isna(),'label'] = timeseries_df.loc[timeseries_df['label'].isna(),label]  

        for label in [l for l in timeseries_df.columns if ('label' in l .lower()) and (l != 'label')]: 
            timeseries_df = timeseries_df.drop(label,axis=1)

        return timeseries_df

#### RUN CODE




#ticker_list = next(os.walk(csv_path))[1]
#ticker_list = ['ZM'] 


#for ticker in ticker_list:

#logfilename = f"{log_path}csv_to_timeseries_{datetime.now().strftime('%Y_%m_%d__%H_%M')}.log"
#logging.basicConfig(filename=logfilename, filemode='w', format='%(levelname)s - %(message)s',level=logging.INFO)
#self.statement_dict = pick_latest_statement(csv_path,ticker)

#print_and_log(logging,ticker)

        
