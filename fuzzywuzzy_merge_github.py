import pandas as pd
import glob, os
import numpy as np
import xlrd
import string
from fuzzywuzzy import fuzz
import difflib

"""Fuzzy matching helper function,
Input 1: csv file with a field you want to fuzzy match to
Input 2: another csv file with a corresponding field
Output: csv with the 2 fuzzy-matched fields
Prior to using this function, need to: 
1) define directory location of both csv's,
2) lowercase and remove special characters and punctuations from both fields"""

matchfile1='csv1.csv'
matchfile1_field_name='NAMELAST'
matchfile2='csv2.csv'
matchfile2_field_name='NAMELAST'
matchfile1_df = pd.read_csv(matchfile1)
matchfile2_df=pd.read_csv(matchfile2)

#FOR TESTING PURPOSE ONLY, IN CASE YOU WANT TO SHRINK matchfile1 OR matchfile2
# matchfile1_df=matchfile1_df.tail(10000)
# matchfile2_df=matchfile2_df.head(200)

matchfile1_df_notnull=matchfile1_df[pd.notnull(matchfile1_df[matchfile1_field_name])]
matchfile2_df_notnull=matchfile2_df[pd.notnull(matchfile2_df[matchfile2_field_name])]

def fuzzy_match_df(row, accuracy_min,accuracy_max, df_1):
    d = df_1.apply(\
        lambda x: fuzz.token_set_ratio(x[matchfile1_field_name], \
        row[matchfile2_field_name]), axis=1)
    d = d[d >= accuracy_min]
    d = d[d < accuracy_max]
    return d

initial_match=matchfile2_df_notnull.apply(\
    lambda x: fuzzy_match_df(x, accuracy_min=90, accuracy_max=100,\
    df_1=matchfile1_df), axis=1)

#reformat output from fuzzywuzzy package
reindex_initial_match=initial_match.stack().reset_index().rename(
    columns={"level_0": 'index_2', "level_1": 'index_1', "0": 'score'}
).astype({'index_1': np.int64})

reindex_initial_match_merged = reindex_initial_match.merge(
    matchfile1_df_notnull,
    left_on='index_1',
    right_index=True
).merge(
    matchfile2_df_notnull,
    left_on='index_2',
    right_index=True
)

reindex_initial_match_merged.to_csv('matched.csv',header=True,encoding='utf-8')
