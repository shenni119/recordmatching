import pandas as pd
import glob, os
import numpy as np
import xlrd
import string
from fuzzywuzzy import fuzz
import difflib

output_customer_aging_combined_noemailphone='output_customer_aging_combined_noemailphone.csv'
output_customer_aging_combined_withemailphone='output_customer_aging_combined_withemailphone2.csv'
output_permit_license_combined='output_permit_license_combined724.csv'
input_dept_budgetcode='budgetcode_key.csv'

agingcsv=output_customer_aging_combined_withemailphone
permit_license_csv=output_permit_license_combined

aging_df = pd.read_csv(agingcsv)
permit_license_df=pd.read_csv(permit_license_csv)

headers_universal=['APKEY',"APNO",'DESCRIPT','FEEDESC','BUDGCODE','Customer ID','Unit','ADDDTTM','NAMELAST_x','NAMELAST_y','ADDR1_y','Address_aging','Over60days']
# shortcut=['DAYPHN','EMAIL','ADDR1','NAMELAST','MOBILE','FULLNAME','EVEPHN','ADDR2']

header_dayphone=headers_universal+['DAYPHN']
header_email=headers_universal+['EMAIL']
header_addr=headers_universal+['ADDR1']
header_name=headers_universal+['NAMELAST']
header_mobile=headers_universal+['MOBILE']
header_fullname=headers_universal+['FULLNAME']
header_evenphn=headers_universal+['EVEPHN']
header_addr2=headers_universal+['ADDR2']

same_dayphonenum=aging_df[pd.notnull(aging_df.DAYPHN)].merge(permit_license_df[pd.notnull(permit_license_df.DAYPHN)],on=['DAYPHN'], how='inner')
# same_dayphonenum.to_csv('same_name_filter.csv', header=True, index=False, encoding='utf-8')
same_dayphonenum=same_dayphonenum.drop_duplicates(['APKEY', 'DESCRIPT', 'FEEDESC', 'Customer ID','Address_aging','DAYPHN'])
# same_dayphonenum.to_csv('same_dayphonenum_test2.csv', header=True, index=False, encoding='utf-8')
same_dayphonenum_filter=same_dayphonenum[header_dayphone]
same_dayphonenum_filter.to_csv('same_dayphonenum_filter.csv', header=True, index=False, encoding='utf-8')

same_name=aging_df[pd.notnull(aging_df.NAMELAST)].merge(permit_license_df[pd.notnull(permit_license_df.NAMELAST)],on=['NAMELAST'], how='inner')
same_name=same_name.drop_duplicates(['APKEY', 'DESCRIPT', 'FEEDESC','Customer ID','Address_aging', 'NAMELAST'])
# same_name.to_csv('same_name2.csv', header=True, index=False, encoding='utf-8',columns = header_name)
same_name['NAMELAST_x']=same_name['NAMELAST']
same_name['NAMELAST_y']=same_name['NAMELAST']
same_name_filter=same_name[header_name]
same_name_filter.to_csv('same_name_filter.csv', header=True, index=False, encoding='utf-8')

same_email=aging_df[pd.notnull(aging_df.EMAIL)].merge(permit_license_df[pd.notnull(permit_license_df.EMAIL)],on=['EMAIL'], how='inner')
same_email=same_email.drop_duplicates(['APKEY', 'DESCRIPT', 'FEEDESC','Customer ID','Address_aging', 'EMAIL'])
# same_email.to_csv('same_email2.csv', header=True, index=False, encoding='utf-8',columns = header_email)
same_email_filter=same_email[header_email]
same_email_filter.to_csv('same_email_filter.csv', header=True, index=False, encoding='utf-8')

same_mobile=aging_df[pd.notnull(aging_df.MOBILE)].merge(permit_license_df[pd.notnull(permit_license_df.MOBILE)],on=['MOBILE'], how='inner')
same_mobile=same_mobile.drop_duplicates(['APKEY', 'DESCRIPT', 'FEEDESC','Customer ID','Address_aging', 'MOBILE'])
same_mobile_filter=same_mobile[header_mobile]
same_mobile_filter.to_csv('same_mobile_filter.csv', header=True, index=False, encoding='utf-8')

same_fullname=aging_df[pd.notnull(aging_df.FULLNAME)].merge(permit_license_df[pd.notnull(permit_license_df.FULLNAME)],on=['FULLNAME'], how='inner')
same_fullname=same_fullname.drop_duplicates(['APKEY', 'DESCRIPT', 'FEEDESC','Customer ID','Address_aging', 'FULLNAME'])
same_fullname_filter=same_fullname[header_fullname]
same_fullname_filter.to_csv('same_fullname_filter.csv', header=True, index=False, encoding='utf-8')


same_evephn=aging_df[pd.notnull(aging_df.EVEPHN)].merge(permit_license_df[pd.notnull(permit_license_df.EVEPHN)],on=['EVEPHN'], how='inner')
same_evephn=same_evephn.drop_duplicates(['APKEY', 'DESCRIPT', 'FEEDESC','Customer ID','Address_aging', 'EVEPHN'])
same_evephn_filter=same_evephn[header_evenphn]
same_evephn_filter.to_csv('same_evephn_filter.csv', header=True, index=False, encoding='utf-8')

#ADDRESS IS NOT USED AS AN IDENTIFIER BECAUSE MORE THAN 1 BUSINESS CAN BE AT AN ADDRESS, AND HIGH ERROR RATES#
# same_addr2=aging_df[pd.notnull(aging_df.ADDR2)].merge(permit_license_df[pd.notnull(permit_license_df.ADDR2)],on=['ADDR2'], how='inner')
# same_addr2=same_addr2.drop_duplicates(['APKEY', 'DESCRIPT', 'FEEDESC','Customer ID','Address_aging', 'ADDR2'])
# same_addr2_filter=same_addr2[header_addr2]
# same_addr2_filter.to_csv('same_addr2_filter.csv', header=True, index=False, encoding='utf-8')

# permit_license_df_addr=permit_license_df
# permit_license_df_addr['ADDR1_y']=permit_license_df_addr['ADDR1']
# same_addr=aging_df[pd.notnull(aging_df.ADDR1)].merge(permit_license_df_addr[pd.notnull(permit_license_df_addr.ADDR1)],on=['ADDR1'], how='inner')
# same_addr=same_addr.drop_duplicates(['APKEY', 'DESCRIPT', 'FEEDESC','Customer ID','Address_aging', 'ADDR1'])
# # same_addr.to_csv('same_addr_filter_test.csv', header=True, index=False, encoding='utf-8')
# same_addr_filter=same_addr[header_addr]
# same_addr_filter.to_csv('same_addr_filter.csv', header=True, index=False, encoding='utf-8')

same_dayphonenum_filter.rename(columns={'DAYPHN': 'indentifier'}, inplace=True)
same_name_filter.rename(columns={'NAMELAST': 'indentifier'}, inplace=True)
same_email_filter.rename(columns={'EMAIL': 'indentifier'}, inplace=True)
same_mobile_filter.rename(columns={'MOBILE': 'indentifier'}, inplace=True)
same_fullname_filter.rename(columns={'FULLNAME': 'indentifier'}, inplace=True)
same_evephn_filter.rename(columns={'EVEPHN': 'indentifier'}, inplace=True)
# same_dayphonenum_filter['indentifier']=same_dayphonenum_filter['DAYPHN']
# same_addr_filter['indentifier']=same_addr_filter['ADDR1']
# same_name_filter['indentifier']=same_name_filter['NAMELAST']
# same_email_filter['indentifier']=same_email_filter['EMAIL']
# same_mobile_filter['indentifier']=same_mobile_filter['MOBILE']
# same_fullname_filter['indentifier']=same_fullname_filter['FULLNAME']
# same_evephn_filter['indentifier']=same_evephn_filter['EVEPHN']


same_frames = [same_dayphonenum_filter,same_name_filter,same_email_filter,same_mobile_filter,same_fullname_filter,same_evephn_filter]

for frame in same_frames:
    frame.rename(columns={'Unit':'BUDGCODE_aging'}, inplace=True)
    # print(same_email.head())
same_combined_df = pd.concat(same_frames)
same_combined_df_dup_reduce=same_combined_df.drop_duplicates(subset=['APKEY','DESCRIPT','FEEDESC','Customer ID','BUDGCODE_aging','Address_aging'])

same_combined_df_dup_reduce.to_csv('same_combined_df_dup_reduce.csv',header=True,encoding='utf-8')


dept_budgetcode_pd = pd.read_csv(input_dept_budgetcode, dtype = {'Code': str})
dept_budgetcode_pd.rename(columns={'Code': 'BUDGCODE'}, inplace=True)
dept_budgetcode_pd.rename(columns={'Unit': 'BUDGCODE'}, inplace=True)
# dept_budgetcode_df=dept_budgetcode_pd.parse()
output_same_combined_df=same_combined_df_dup_reduce[pd.notnull(same_combined_df_dup_reduce.BUDGCODE)].merge(
                        dept_budgetcode_pd[pd.notnull(dept_budgetcode_pd.BUDGCODE)],on=['BUDGCODE'], how='left')

output_same_combined_df['same_dept'] = np.where((output_same_combined_df['BUDGCODE'] == output_same_combined_df['BUDGCODE_aging'])
                                        , 1, 0)

output_same_combined_df.rename(columns={'Office': 'office_hansen','BUDGCODE':'BUDGCODE_hansen','NAMELAST_x':'name_aging',"NAMELAST_y":'name_hansen','Customer ID':'Customer ID_aging','ADDR1_y':'Address_hansen','ADDDTTM':'Application_addtime_hansen'}, inplace=True)
remove_budgetcode_redundancy=['APKEY','APNO','DESCRIPT','FEEDESC','BUDGCODE_hansen','name_aging','name_hansen','BUDGCODE_aging','Over60days','Address_aging','Address_hansen','Customer ID_aging','Application_addtime_hansen','indentifier']
output_same_combined_df['agencies_involved']=output_same_combined_df['office_hansen'].astype(str)
output_pivottable_same_combined_df=output_same_combined_df.groupby(remove_budgetcode_redundancy)['agencies_involved'].apply(' '.join).reset_index()
# output_pivottable_same_combined_df["BUDGET_FEE_DESCR"] = output_pivottable_same_combined_df["DESCRIPT"].map(str) +' '+ output_pivottable_same_combined_df["FEEDESC"]



output_pivottable_same_combined_df.to_csv('combined_data_by_hansen_transaction9.csv',header=True,encoding='utf-8')



#
# output_same_combined_df.rename(columns={'Office': 'office_hansen','BUDGCODE':'BUDGCODE_hansen','NAMELAST_x':'name_aging',"NAMELAST_y":'name_hansen','Customer ID':'Customer ID_aging','ADDR1_y':'Address_hansen','ADDDTTM':'Application_addtime_hansen'}, inplace=True)
# remove_budgetcode_redundancy=['APKEY','APNO','DESCRIPT','FEEDESC','BUDGCODE_hansen','name_aging','name_hansen','BUDGCODE_aging','Over60days','Address_aging','Address_hansen','Customer ID_aging','Application_addtime_hansen','indentifier']
#

# #In [11]: df.groupby(['col5', 'col2']).size()
# # print (output_pivottable_same_combined_df({'count': output_pivottable_same_combined_df.groupby(["BUDGET_FEE_DESCR",'BUDGCODE_hansen']).size()}).head())
#
# # print (most_valuable_hansen_process.head())
#
# # hansen_uniquekey.append('frequency')
# # hansen_interactions_by_ar_account2=output_pivottable_same_combined_df.groupby(
# # hansen_uniquekey)['ADDDTTM'].max().to_frame().reset_index()
# output_pivottable_same_combined_df['ADDDTTM']=pd.to_datetime(output_pivottable_same_combined_df['ADDDTTM'])
# output_pivottable_same_combined_df.sort_values(['Over60days','Customer ID','APKEY'],inplace=True, ascending=[False,False, False])
#
# hansen_uniquekey=['Customer ID','name_aging','Address_aging','Over60days','BUDGCODE_aging','BUDGCODE_hansen','BUDGET_FEE_DESCR']
# hansen_interactions_by_ar_account_count=output_pivottable_same_combined_df.groupby(
# hansen_uniquekey).size().to_frame(name = 'frequency').reset_index()
# hansen_interactions_by_ar_account_latest_interactions=output_pivottable_same_combined_df.groupby(hansen_uniquekey)['ADDDTTM'].nlargest(5).to_frame(name = 'max').reset_index()
# hansen_interactions_by_ar_account_latest_interactions['str_max']=hansen_interactions_by_ar_account_latest_interactions['max'].astype(str)
# hansen_interactions_by_ar_account_latest_interactions=hansen_interactions_by_ar_account_latest_interactions.groupby(hansen_uniquekey)['str_max'].apply(', '.join).to_frame(name = 'str_max_combined').reset_index()
# hansen_interactions_by_ar_account_merged= pd.merge(hansen_interactions_by_ar_account_latest_interactions, hansen_interactions_by_ar_account_count,  how='inner', on=hansen_uniquekey)
#
# hansen_interactions_by_ar_account_merged.to_csv('aging_accounts_by_hansen_interaction.csv',header=True,encoding='utf-8')
# # print (hansen_interactions_by_ar_account_merged.head())
#
# # hansen_interactions_by_ar_account_testing=hansen_interactions_by_ar_account_merged.groupby(unique_aging_account)
# # hansen_interactions_by_ar_account_testing['most_freq']=
# # ['str_max_combined'].size()
# #
# # print (hansen_interactions_by_ar_account_testing.head())
# hansen_interactions_by_ar_account_merged["Customer_key"] = hansen_interactions_by_ar_account_merged["Customer ID"].map(str) +' '+ hansen_interactions_by_ar_account_merged["Address_aging"]
# most_freq_hansen_interactions_by_ar_account=hansen_interactions_by_ar_account_merged.groupby("Customer_key")
# most_freq_hansen_interactions_by_ar_account = most_freq_hansen_interactions_by_ar_account.apply(
#                     lambda x: x[x['frequency'] == x['frequency'].max()])
# most_freq_hansen_interactions_by_ar_account.rename(columns={'str_max_combined': 'most_frequent'}, inplace=True)
#
# hansen_interactions_by_ar_account_merged= pd.merge(hansen_interactions_by_ar_account_latest_interactions, hansen_interactions_by_ar_account_count,  how='inner', on=hansen_uniquekey)
#
# # hansen_interactions_by_ar_account_testing2 = hansen_interactions_by_ar_account_testing2.agg({'C' : np.sum,'D' : lambda x: np.std(x, ddof=1)})
#
#
# # print (hansen_interactions_by_ar_account_testing2.head())
# # print(hansen_interactions_by_ar_account_testing2.reset_index().head())
#
#
# # hansen_interactions_by_ar_account_testing3=hansen_interactions_by_ar_account_testing2.loc[(hansen_interactions_by_ar_account_testing2['frequency'] == hansen_interactions_by_ar_account_testing2['frequency'].max())]
# # print (hansen_interactions_by_ar_account_testing3.head())
#
# # hansen_interactions_by_ar_account_testing2.to_csv('hansen_interactions_by_ar_account_testing3.csv')
