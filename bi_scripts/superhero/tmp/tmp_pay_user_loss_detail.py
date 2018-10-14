#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Dong Junshuang
Description : 付费玩家流失情况
'''
import settings
from utils import hqls_to_dfs, update_mysql, ds_add
import pandas as pd

settings.set_env('superhero_vt')
act_start_date = '20160718'
act_end_date = '20160731'
start_date = '20160801'
end_date = '20160831'

act_info_sql = '''
SELECT uid,
       max(vip_level) vip_level
FROM raw_info
WHERE ds >= '{act_start_date}'
  AND ds <='{act_end_date}'
  and vip_level >=1
GROUP BY uid
'''.format(act_start_date=act_start_date,
           act_end_date=act_end_date)
spend_sql = '''
SELECT uid,
       sum(order_coin) coin_num
FROM raw_paylog
WHERE ds >= '{start_date}'
  AND ds <='{end_date}'
GROUP BY uid
'''.format(start_date=start_date, end_date=end_date)
info_sql = '''
SELECT uid,
       count(distinct ds) act_day
FROM raw_info
WHERE ds >= '{start_date}'
  AND ds <='{end_date}'
GROUP BY uid
'''.format(start_date=start_date, end_date=end_date)

act_info_df, spend_df, info_df = hqls_to_dfs(
    [act_info_sql, spend_sql, info_sql])
result_df = (
    act_info_df.merge(spend_df, on='uid', how='left')
    .merge(info_df, on='uid', how='left')
    .fillna(0))

result_df = result_df.sort_values(by='vip_level')
column = ['uid','vip_level','coin_num','act_day']
result_df = result_df[column]

result_df.to_excel('/Users/kaiqigu/Downloads/Excel/pay_uid_loss_detail.xlsx')
