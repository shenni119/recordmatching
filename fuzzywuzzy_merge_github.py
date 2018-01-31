import pandas as pd
import glob, os
import numpy as np
import xlrd
import string
from fuzzywuzzy import fuzz
import difflib

# output_customer_aging_combined_noemailphone='output_customer_aging_combined_noemailphone.csv'
matchfile1='output_customer_aging_combined_withemailphone2.csv'
matchfile2='checkpoint_permit_license_combined.csv'
# input_dept_budgetcode='budgetcode_key.csv'

matchfile1_df = pd.read_csv(matchfile1)
matchfile2_df=pd.read_csv(matchfile2)

permit_license_df10000=matchfile1_df.tail(10000)
aging_df100=matchfile2_df.head(200)

permit_license_df10000_notnull=permit_license_df10000[pd.notnull(permit_license_df10000.NAMELAST)]
aging_df100_notnull=aging_df100[pd.notnull(aging_df100.NAMELAST)]

permit_license_df10000_notnull.to_csv('dataframe1.csv',header=True,encoding='utf-8')
aging_df100_notnull.to_csv('dataframe2.csv',header=True,encoding='utf-8')

# headers_universal=['APKEY',"APNO",'DESCRIPT','FEEDESC','BUDGCODE','Customer ID','ADDDTTM','NAMELAST']
# hansen_specific_headers=['Hansen_APKEY',"Hansen_APNO",'Hansen_DESCRIPT','Hansen_FEEDESC','Hansen_BUDGCODE','Hansen_Customer ID','Hansen_ADDDTTM','Hansen_NAMELAST']

def get_permit_licenses(row):
    d = permit_license_df10000_notnull.apply(lambda x: fuzz.token_set_ratio(x['NAMELAST'], row['NAMELAST']), axis=1)
    d = d[d >= 90]
    d = d[d < 100]
    return d

initial_match=aging_df100_notnull.apply(get_permit_licenses, axis=1)
reindex_initial_match=initial_match.stack().reset_index().rename(
    columns={"level_0": 'index_2', "level_1": 'index_1', "0": 'score'}
).astype({'index_1': np.int64})

reindex_initial_match_merged = reindex_initial_match.merge(
    permit_license_df10000_notnull,
    left_on='index_1',
    right_index=True
).merge(
    aging_df100_notnull,
    left_on='index_2',
    right_index=True
)

reindex_initial_match_merged.to_csv('matched_test4.csv',header=True,encoding='utf-8')

# testing3=aging_df100_notnull.apply(get_permit_licenses, axis=1)
# testing3.to_csv('testing3.csv',header=True,encoding='utf-8')

# fuzzycombine=pd.concat((aging_df100_notnull, aging_df100_notnull.apply(get_permit_licenses, axis=1)), axis=1)
# fuzzycombine.to_csv('fuzzycombine.csv',header=True,encoding='utf-8')
