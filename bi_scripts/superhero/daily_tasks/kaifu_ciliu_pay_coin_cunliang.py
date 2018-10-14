#!/usr/bin/env python  
# encoding: utf-8  

""" 
@author: Andy 
@site:  新服开服第N天的注册用户的留存-收入-钻石消耗
@software: PyCharm 
@file: kaifu_ciliu_pay_coin_cunliang.py 
@time: 17/9/19 下午5:33 
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
new_or_old_df = pd.DataFrame(new_or_old_df).dropna()
def card_evo_lines():
    for _, row in new_or_old_df.iterrows():
        now = datetime.datetime.strptime(str(row.reg_time), '%Y%m%d')
        end = datetime.datetime.strptime(str(row.ds), '%Y%m%d')
        delta = ''
        if now <= end:
            delta = (end - now).days+1
        # print [row.ds, row.server, row.reg_time,row.dau, delta]
        yield [row.ds, row.server, row.reg_time,row.dau,delta]

new_or_old_df = pd.DataFrame(card_evo_lines(),columns=['ds', 'server', 'reg_time','dau', 'delta', ])
new_or_old_df = new_or_old_df[new_or_old_df['delta'] <= 7]
print new_or_old_df.head()


act_info_sql = '''
    select ds,uid,reverse(substring(reverse(uid), 8)) AS server,regexp_replace(substring(create_time,1,10),'-','') as reg_ds,zuanshi from raw_info where ds>='20170713' group by ds,uid,server,reg_ds,zuanshi
'''
act_info_df = hql_to_df(act_info_sql)
print act_info_df.head(3)

pay_info_sql = '''
    select ds,uid,sum(order_coin) as order_money from raw_paylog where  ds>='20170713'  and platform_2 != 'admin_test' group by ds,uid
'''
pay_info_df = hql_to_df(pay_info_sql)
print pay_info_df.head(3)

coin_spend_sql = '''
    select ds,uid,sum(coin_num) as coin_num from raw_spendlog where ds>='20170713' group by ds,uid
'''
coin_spend_df = hql_to_df(coin_spend_sql)
print coin_spend_df.head(3)

result_df = act_info_df.merge(new_or_old_df, on=['ds','server',], how='left')
result_df = result_df.merge(pay_info_df, on=['ds','uid',], how='left')
result_df = result_df.merge(coin_spend_df, on=['ds','uid',], how='left')
print result_df.head()

is_gun_pay_reg_df = result_df.groupby(['server','reg_time','reg_ds','delta',]).agg({'order_money': lambda g: g.sum(), 'coin_num': lambda g: g.sum(),'zuanshi': lambda g: g.sum(),'uid': lambda g: g.nunique()}).reset_index()

#  新服开服第N天的注册用户的留存-收入-钻石消耗
is_gun_pay_reg_df.to_excel('/Users/kaiqigu/Documents/Superhero/超级英雄-越南-开服n天_钻石存量-钻石消耗-留存-收入(钻石)_20170919.xlsx')
