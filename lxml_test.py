 

import string
from sklearn.metrics.pairwise import cosine_similarity
from sklearn.feature_extraction.text import CountVectorizer
from nltk.corpus import stopwords
stopwords = stopwords.words('english')

import pandas as pd
from time import time
import os
import numpy as np 

def clean_string(text):
    text = ''.join([word for word in text if word not in string.punctuation])
    text = text.lower()
    text = ' '.join([word for word in text.split() if word not in stopwords])
    return text

def cosine_sim_vectors(vec1,vec2):
    vec1 = vec1.reshape(1,-1)
    vec2 = vec2.reshape(1,-1)
    return cosine_similarity(vec1,vec2)[0][0]


def pick_latest_statement(csv_path,ticker):
    ded_dict = {}
    for dirname in os.listdir(f"{csv_path}{ticker}"):
        ded_dict[dirname[:10]] = dirname 
    
    return ded_dict[max(ded_dict.keys())]





csv_path = '../data/csv/'
ticker = 'AMZN'

latest_ded = pick_latest_statement(csv_path,ticker)

statement = '1002000 - Statement - Consolidated Statements of Operations.csv'
reference_df = pd.read_csv('../data/csv/AMZN/2020-03-31 (10-Q)/1002000 - Statement - Consolidated Statements of Operations.csv',index_col=[0])


example_dirs = ['2020-03-31 (10-Q)','2019-12-31 (10-K)', '2009-12-31 (10-K)', '2014-09-30 (10-Q)']
example_dirs = ['2009-12-31 (10-K)']

df_dict = {}
df_dict['2020-03-31 (10-Q)'] = pd.read_csv('../data/csv/AMZN/2020-03-31 (10-Q)/1002000 - Statement - Consolidated Statements of Operations.csv',index_col=[0])
latest_ded = '2020-03-31 (10-Q)' 

start_time = time()
missing_list = []
for dirname in example_dirs:
    dirname = f"{csv_path}{ticker}/{dirname}"
    filenames = os.listdir(f"{dirname}")

    filenames.insert(0,statement)

    missing_list = list(missing_list)
    missing_list.append(dirname)

    cleaned_statement_names = list(map(clean_string,filenames))
    vectorizer = CountVectorizer().fit_transform(cleaned_statement_names)
    statement_name_vectors = vectorizer.toarray()
    csim_matrix = cosine_similarity(statement_name_vectors)
    csim_vector = csim_matrix[0] 
    print(csim_matrix)
    close_enough_index = np.where(csim_vector>0.3)

    max_overlap = 0

    for i in close_enough_index[0][1:]:
        comp_df = pd.read_csv(os.path.join(dirname,filenames[i]),index_col=[0])
        overlap_percentage = len(set(df_dict[latest_ded].index).intersection(comp_df.index))/len(df_dict[latest_ded])  
        print(f"{filenames[i]} {csim_vector[i]}")
        if overlap_percentage > max_overlap:
            max_overlap = overlap_percentage
            most_likely_statement_match = filenames[i]

    if max_overlap > 0.6:
        full_path = os.path.join(dirname,most_likely_statement_match)
        df_dict[full_path[17:34]] = pd.read_csv(full_path,index_col=[0])
        missing_list.remove(dirname)

print(f"new method {time()-start_time}")


missing_list = []
start_time = time()
for dirname in example_dirs:
    dirname = f"{csv_path}{ticker}/{dirname}"  
    max_overlap = 0
    for filename in os.listdir(dirname): 
        if (filename[-4:] == '.csv'): 
            missing_list = list(missing_list)
            missing_list.append(dirname)

            start_time = time()
            statement_names = [statement, filename]
            cleaned_statement_names = list(map(clean_string,statement_names))
            vectorizer = CountVectorizer().fit_transform(cleaned_statement_names)
            statement_name_vectors = vectorizer.toarray()
            csim = cosine_sim_vectors(statement_name_vectors[0],statement_name_vectors[1]) 
            if csim == 1:
                most_likely_statement_match = filename 
                max_overlap = 1
                break #if filenames map don't even bother looking for column overlap
            elif csim > 0.3:
                print(f"{dirname} {filename} {csim}") 
                comp_df = pd.read_csv(os.path.join(dirname,filename),index_col=[0])
                overlap_percentage = len(set(df_dict[latest_ded].index).intersection(comp_df.index))/len(df_dict[latest_ded])  
            
                if overlap_percentage > max_overlap:
                    max_overlap = overlap_percentage
                    most_likely_statement_match = filename

    if max_overlap > 0.6:
        full_path = os.path.join(dirname,most_likely_statement_match)
        df_dict[full_path[17:34]] = pd.read_csv(full_path,index_col=[0])
        missing_list.remove(dirname)

    missing_list = np.unique(missing_list)

print(f"current method {time() - start_time}")