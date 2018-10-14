#!/usr/bin/env python  
# encoding: utf-8  

""" 
@author: Andy 
@site:  开服第n天的钻石消耗
@software: PyCharm 
@file: kaifu_coin_spend.py 
@time: 17/9/19 下午5:08 
"""

import settings_dev
from utils import ds_add
from utils import hql_to_df
from utils import update_mysql
from sqls_for_games.superhero import gs_sql
import pandas as pd
import time
import datetime

settings_dev.set_env('superhero_vt')
# 新老服的开服天数
new_or_old_sql = '''
SELECT reverse(substring(reverse(uid), 8)) AS server,
       regexp_replace(substr(min(create_time),1,10),'-','') AS reg_time
FROM mid_info_all
WHERE ds ='20170918'
  AND create_time <> '1970-01-01 07:00:00'
GROUP BY reverse(substring(reverse(uid), 8))
'''
new_or_old_df = hql_to_df(new_or_old_sql)
ds_new_or_old_sql = '''
    SELECT ds,reverse(substring(reverse(uid), 8)) AS server,count(distinct uid) as dau
    FROM raw_info
    WHERE ds >='20170713'
      AND create_time <> '1970-01-01 07:00:00'
    GROUP BY ds,reverse(substring(reverse(uid), 8))
    '''
ds_new_or_old_df = hql_to_df(ds_new_or_old_sql)
print ds_new_or_old_df.head(3)
new_or_old_df = ds_new_or_old_df.merge(new_or_old_df, on=['server', ], how='left')

def card_evo_lines():
    for _, row in new_or_old_df.iterrows():
        now = datetime.datetime.strptime(row.reg_time, '%Y%m%d')
        end = datetime.datetime.strptime(row.ds, '%Y%m%d')
        delta = ''
        if now <= end:
            delta = (end - now).days+1
        # print [row.ds, row.server, row.reg_time,row.dau, delta]
        yield [row.ds, row.server, row.reg_time,row.dau,delta]

new_or_old_df = pd.DataFrame(card_evo_lines(),columns=['ds', 'server', 'reg_time','dau', 'delta', ])
new_or_old_df = new_or_old_df[new_or_old_df['delta'] <= 7]
print new_or_old_df.head()


coin_sql = '''
    select ds,reverse(substring(reverse(uid), 8)) AS server,sum(coin_num) as coin_num from raw_spendlog where ds>='20170713' group by ds,server
'''
coin_df = hql_to_df(coin_sql)
print coin_df.head(3)

result_df = coin_df.merge(new_or_old_df, on=['ds','server',], how='left')
result_df.to_excel('/Users/kaiqigu/Documents/Superhero/超级英雄-越南-开服n天钻石消耗_20170919.xlsx')