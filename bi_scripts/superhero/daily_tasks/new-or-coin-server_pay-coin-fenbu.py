#!/usr/bin/env python  
# encoding: utf-8  

""" 
@author: Andy 
@site:  
@software: PyCharm 
@file: new-or-coin-server_pay-coin-fenbu.py 
@time: 17/9/19 下午2:47 
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
print new_or_old_df.head(3)

#分服每天的钻石消耗-充值
order_coin_sql = '''
    select t1.ds,t1.server,case when t1.order_coin<=100 then '-100'
        when t1.order_coin<=500 then '100-500'
        when t1.order_coin<=1500 then '500-1500'
        when t1.order_coin<=5000 then '1500-5000'
        when t1.order_coin<=20000 then '5000-20000'
        else '20000' end as order_coin_fenbu,count(distinct t1.uid) as uid_num 
    from (
        select ds,uid,reverse(substring(reverse(uid),8))as server,sum(order_coin) as order_coin from raw_paylog where ds>='20170713' and platform_2 != 'admin_test' 
        group by ds,uid,server
    )t1
    group by t1.ds,t1.server,order_coin_fenbu
'''
order_coin_df = hql_to_df(order_coin_sql)
print order_coin_df.head(3)

result = order_coin_df.merge(new_or_old_df, on=['server', ], how='left')

def card_evo_lines():
    for _, row in result.iterrows():
        now = datetime.datetime.strptime(row.reg_time, '%Y%m%d')
        end = datetime.datetime.strptime(row.ds, '%Y%m%d')
        delta= 10000000000
        if now <= end:
            delta = (end - now).days+1
        print [row.ds,row.server, row.order_coin_fenbu, row.uid_num,row.reg_time,delta]
        yield [row.ds,row.server, row.order_coin_fenbu, row.uid_num,row.reg_time,delta]

result = pd.DataFrame(card_evo_lines(),columns=['ds', 'server', 'order_coin_fenbu', 'uid_num','reg_time', 'delta', ])
result = result[result['delta'] <= 7]
print result.head()

result.to_excel('/Users/kaiqigu/Documents/Superhero/超级英雄-越南-充值钻石等级分布_20170919-2.xlsx')