#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Dong Junshuang
Description : 钻石存量(pub)
'''
import settings
from utils import hqls_to_dfs, update_mysql, ds_add
import pandas as pd

settings.set_env('superhero_bi')
start_date = '20160927'
end_date = '20161007'
info_sql = '''
SELECT ds,
       count(uid) sum_uid,
       sum(zuanshi) sum_coin
FROM raw_info
WHERE substr(uid,1,1) = 'g'
  AND ds >= '{start_date}'
  AND ds<='{end_date}'
GROUP BY ds
'''.format(start_date=start_date,end_date = end_date)
spend_sql = '''
SELECT ds,
       sum(coin_num) spend_num
FROM raw_spendlog
WHERE substr(uid,1,1) = 'g'
  AND ds >= '{start_date}'
  AND ds<='{end_date}'
GROUP BY ds
'''.format(start_date=start_date,end_date = end_date)
info_df,spend_df = hqls_to_dfs([info_sql,spend_sql])
result_df = info_df.merge(spend_df,on = 'ds',how='outer')
result_df = result_df.sort_values(by='ds')
column = ['ds','sum_coin','spend_num','sum_uid']
result_df = result_df[column]

result_df.to_excel('/Users/kaiqigu/Downloads/Excel/coin_cunliang.xlsx')
