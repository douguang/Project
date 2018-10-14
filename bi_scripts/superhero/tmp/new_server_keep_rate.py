#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Dong Junshuang
Description : 新服数据-留存
备注：第一天的次日留存不准，待改进
'''
import pandas as pd
from utils import date_range, hql_to_df,ds_add
import settings

settings.set_env('superhero_bi')
name ='g555'
date1 ='20160925'
date2 ='20160929'
keep_list = [2,3,7]
reg_dates = []
keep_date_list = set()
while date1 <= date2:
    reg_dates.append(date1)
    for d,t in enumerate(keep_list):
        keep_date_list.add(ds_add(date1,t))
    date1 = ds_add(date1,1)
date1 ='20160924'
reg_sql = '''
SELECT ds,
       uid
FROM raw_reg
WHERE ds >='{date1}'
  AND ds <= '{date2}'
  AND reverse(substring(reverse(uid), 8)) = '{name}'
'''.format(name=name,date1=date1,date2=date2)
info_sql = '''
SELECT ds,
       uid
FROM raw_info
WHERE reverse(substring(reverse(uid), 8)) = '{name}'
  AND ds in {date_list}
'''.format(name=name,date_list=tuple(keep_date_list))
active_df = hql_to_df(info_sql)
reg_df = hql_to_df(reg_sql)
active_df['act'] = 1
reg_act_df = (active_df
                  .pivot_table('act', ['uid'], 'ds')
                  .reset_index()
                  .merge(reg_df,on = ['uid'], how='right')
                  .reset_index()
                  )

# 求每一个受影响的日期留存率，然后合并
keep_rate_dfs = []
for reg_date in reg_dates:
    act_dates = [ds_add(reg_date, keep_day - 1) for keep_day in keep_list]
    act_dates_dic = {ds_add(reg_date, keep_day - 1): 'd%d_keep' % keep_day for keep_day in keep_list}
    keep_df = (reg_act_df[reg_act_df['ds'] == reg_date]
                .loc[:,['ds']+act_dates]
                .groupby('ds')
                .sum()
                .reset_index()
                .fillna(0)
                .rename(columns=act_dates_dic)
               )
    keep_df['reg'] = reg_act_df[reg_act_df['ds'] == reg_date].count().uid
    for c in act_dates_dic.values():
        keep_df[c + 'rate'] = keep_df[c] / keep_df.reg
    keep_rate_dfs.append(keep_df)

keep_rate_df = pd.concat(keep_rate_dfs)
columns = ['ds', 'reg'] + ['d%d_keeprate' % d for d in keep_list]
result_df = keep_rate_df[columns]
print result_df

keep_rate_df.to_excel(r'E:\My_Data_Library\superhero\2016-09-27\{0}_liucun.xlsx'.format(name))



