import pandas as pd
csv_path = '../data/csv/'
ticker = 'AMZN'

df_dict = {}

import os
for dirname, _, filenames in os.walk(f"{csv_path}{ticker}"):
    if '(10-Q)' in dirname:
        for filename in filenames:
            #print(filename)
            if filename.lower() == '1002000 - Statement - Consolidated Statements Of Operations.csv'.lower():
                full_path = os.path.join(dirname, filename)
                print(full_path)
                df_dict[full_path[17:27]] = pd.read_csv(full_path,index_col=[0])


date_list = sorted(list(df_dict.keys()),reverse=True)

for date in date_list:
    if date == date_list[0]:
        master_df = df_dict[date]
    else:
        #df_dict.
        new_columns = df_dict[date].columns.difference(master_df.columns)
        master_df = master_df.merge(df_dict[date][new_columns],left_index=True,right_index=True,how='outer')
        master_df.loc[master_df.index.isin(df_dict[date].index),master_df.columns.isin(df_dict[date].columns)] = df_dict[date]


for metric in master_df[(master_df.isna().any(axis=1) & master_df[date_list[0]].notna())].index:
    print(metric)
    if metric == 'us-gaap_revenuefromcontractwithcustomerexcludingassessedtax':
        print(metric)
    for metric_match in master_df[master_df.index != metric].index:
        if metric_match == 'us-gaap_salesrevenuenet':
            print(metric_match)

        agreement_series = master_df.loc[metric,master_df.notna().all(axis=0)]==master_df.loc[metric_match,master_df.notna().all(axis=0)]
        print(agreement_series)
        if (agreement_series.all() == True) and len(agreement_series > 1):
                print(metric_match)
                master_df.loc[metric,master_df.columns[master_df[master_df.index == metric].isna().iloc[0].to_list()]
 ] = master_df.loc[metric_match,master_df.columns[master_df[master_df.index == metric].isna().iloc[0].to_list()]
 ] 