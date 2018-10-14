#!/usr/bin/env python  
# encoding: utf-8  

""" 
@author: Andy 
@site:  
@software: PyCharm 
@file: new_server_pay_data-ios.py.py 
@time: 17/8/21 下午4:04 
"""


from utils import hqls_to_dfs, get_rank, hql_to_df, date_range
import pandas as pd
from utils import ds_delta
import settings_dev
from pandas import DataFrame

settings_dev.set_env('superhero_bi')

server_sql = '''
SELECT reverse(substring(reverse(uid), 8)) AS server,
       regexp_replace(substr(min(create_time),1,10),'-','') AS reg_time
FROM mid_info_all
WHERE ds ='20170820'
  AND create_time <> '1970-01-01 07:00:00'
  AND account like 'cmgeapp%'
  AND create_time <> '0'
  AND create_time <> '1'
GROUP BY reverse(substring(reverse(uid), 8))
'''
pay_sql = '''
SELECT ds,
       reverse(substring(reverse(uid), 8)) AS server ,
       count(DISTINCT uid) AS pay_num ,
       sum(order_money) AS pay_money
FROM raw_paylog
WHERE ds >='20170701'
  AND ds <='20170820'
  AND platform_2 <> 'admin'
  AND platform_2 <> 'admin_test'
  AND uid in (
    select uid from mid_info_all where ds='20170820' and account like 'cmgeapp%' group by uid
  )
GROUP BY ds,
         reverse(substring(reverse(uid), 8))
'''
info_sql = '''
SELECT ds,
       count(uid) AS dau
FROM raw_info
WHERE ds >='20170701'
AND account like 'cmgeapp%'
  AND ds<='20170820'
GROUP BY ds
'''
server_df = hql_to_df(server_sql).dropna().drop_duplicates()
print server_df
pay_df = hql_to_df(pay_sql).dropna().drop_duplicates()
print pay_df
info_df = hql_to_df(info_sql).dropna().drop_duplicates()
print info_df

dfs = []
for date in date_range('20170701', '20170820'):
    pay_day = pay_df[pay_df.ds == date]
    server_df['day'] = server_df['reg_time'].map(lambda s: ds_delta(s, date))
    new_server = server_df[server_df['day'] <= 7]
    old_server = server_df[server_df['day'] > 7]
    pay_day['is_new'] = pay_day['server'].isin(new_server.server.values)
    new_pay = pay_day[pay_day['is_new']]
    pay_day['is_old'] = pay_day['server'].isin(old_server.server.values)
    old_pay = pay_day[pay_day['is_old']]
    old_result = old_pay.groupby('ds').sum().reset_index()[['ds', 'pay_num',
                                                            'pay_money']]
    new_result = (new_pay.groupby('ds').sum().reset_index()[['ds', 'pay_num',
                                                             'pay_money']]
                  .rename(columns={'pay_num': 'new_pay_num',
                                   'pay_money': 'new_pay_money'}))
    pay_result = (pay_day.groupby('ds').sum().reset_index()[['ds', 'pay_num',
                                                             'pay_money']]
                  .rename(columns={'pay_num': 'total_pay_num',
                                   'pay_money': 'total_pay_money'}))
    result = (old_result.merge(new_result, on='ds').merge(
        pay_result, on='ds').merge(info_df, on='ds'))
    result = result[['ds', 'dau', 'total_pay_money', 'new_pay_num', 'pay_num',
                     'new_pay_money', 'pay_money']]
    dfs.append(result)

result_df = pd.concat(dfs)
result_df.to_excel('/Users/kaiqigu/Documents/Superhero/ios_new_server_pay_data.xlsx')
