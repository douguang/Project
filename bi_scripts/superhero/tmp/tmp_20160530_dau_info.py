#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Dong Junshuang
Description :
'''
import pandas as pd
from utils import date_range, hqls_to_dfs,ds_add
import settings

settings.set_env('superhero_bi')
date1 = '20160401'
date2 = '20160410'
name ='g497'

reg_sql = "select ds,uid,reverse(substring(reverse(uid), 8)) server from raw_reg where ds>='{0}' and ds <= '{1}' ".format(date1,date2)
act_sql = "select ds,uid,reverse(substring(reverse(uid), 8)) server,vip_level from raw_info where ds>='{0}' and ds <= '{1}' ".format(date1,date2)

reg_df,act_df = hqls_to_dfs([reg_sql,act_sql])

# act_df = act_df[act_df['server'] == name]
# reg_df = reg_df[reg_df['server'] == name]

date1 = '20160331'
for i in range(0,10):
    date1 = ds_add(date1,1)
    print date1
    new_reg = reg_df[reg_df['ds'] == date1]
    dau2_act = act_df[act_df['ds'] == ds_add(date1,1)]
    dau3_act = act_df[act_df['ds'] == ds_add(date1,2)]
    dau7_act = act_df[act_df['ds'] == ds_add(date1,6)]
    new_reg['is_dau2'] = new_reg['uid'].isin(dau2_act.uid.values)
    new_reg['is_dau3'] = new_reg['uid'].isin(dau3_act.uid.values)
    new_reg['is_dau7'] = new_reg['uid'].isin(dau7_act.uid.values)
    dau2_data = new_reg[new_reg['is_dau2']]
    dau2_data = dau2_data.drop_duplicates(['uid'])
    dau2_data = dau2_data.groupby('ds').count().reset_index()
    dau2_data['dau2'] = dau2_data['uid']
    columns = ['ds','dau2']
    dau2_data = dau2_data[columns]

    dau3_data = new_reg[new_reg['is_dau3']]
    dau3_data = dau3_data.drop_duplicates(['uid'])
    dau3_data = dau3_data.groupby('ds').count().reset_index()
    dau3_data['dau3'] = dau3_data['uid']
    columns = ['ds','dau3']
    dau3_data = dau3_data[columns]

    dau7_data = new_reg[new_reg['is_dau7']]
    dau7_data = dau7_data.drop_duplicates(['uid'])
    dau7_data = dau7_data.groupby('ds').count().reset_index()
    dau7_data['dau7'] = dau7_data['uid']
    columns = ['ds','dau7']
    dau7_data = dau7_data[columns]

    result = dau2_data.merge(dau3_data,on=['ds'],how='outer')
    result = result.merge(dau7_data,on=['ds'],how='outer')
    print result

    if date1 == '20160401':
        result_data = result
    else:
        result_data = pd.concat([result_data,result])

print result_data
result_data = result_data.fillna(0)

result_data.to_excel('/Users/kaiqigu/Downloads/Excel/g479_tt.xlsx')

