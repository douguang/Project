#!/usr/bin/env python  
# encoding: utf-8  

""" 
@author: Andy 
@site:  新老服钻石消耗
@software: PyCharm 
@file: new-or-old-server_spend-coin-and-pay.py
@time: 17/9/19 上午11:03 
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
        now = datetime.datetime.strptime(row.reg_time, '%Y%m%d')
        end = datetime.datetime.strptime(row.ds, '%Y%m%d')
        delta = ''
        if now <= end:
            delta = (end - now).days+1
        if delta <= 7:
            delta = 'new'
        else:
            delta = 'old'
        # print [row.ds, row.server, row.reg_time,row.dau, delta]
        yield [row.ds, row.server, row.reg_time,row.dau,delta]

new_or_old_df = pd.DataFrame(card_evo_lines(),columns=['ds', 'server', 'reg_time','dau', 'delta', ])
# new_or_old_df = new_or_old_df[new_or_old_df['delta'] <= 7]
print new_or_old_df.head()

# 充值人数-充值金额
pay_sql = '''
    select ds,uid,reverse(substring(reverse(uid), 8)) AS server,sum(order_money) as order_money from raw_paylog where ds>='20170713' and platform_2 !='admin_test' group by ds,uid,server
'''
pay_df = hql_to_df(pay_sql)
print pay_df.head(3)
pay_df = pay_df.merge(new_or_old_df, on=['ds','server', ], how='left')
pay_df = pay_df.groupby(['ds','server','dau','delta',]).agg({'order_money': lambda g: g.sum(), 'uid': lambda g: g.nunique()}).reset_index()
pay_df = pay_df.rename(columns={'uid': 'uid_pay_num',})

# 钻石消耗
coin_sql = '''
    select ds,uid,reverse(substring(reverse(uid), 8)) AS server,sum(coin_num) as coin_num from raw_spendlog where ds>='20170713' group by ds,uid,server
'''
coin_df = hql_to_df(coin_sql)
print coin_df.head(3)
coin_df = coin_df.merge(new_or_old_df, on=['ds','server', ], how='left')
coin_df = coin_df.groupby(['ds','server','dau','delta',]).agg({'coin_num': lambda g: g.sum(), 'uid': lambda g: g.nunique()}).reset_index()
coin_df = coin_df.rename(columns={'uid': 'uid_spend_coin_num',})


# 新增人数-新增付费人数-新增付费金额
reg_info_sql = '''
    select regexp_replace(substring(create_time,1,10),'-','') as ds,reverse(substring(reverse(uid), 8)) AS server,uid from mid_info_all where ds='20170918' and regexp_replace(substring(create_time,1,10),'-','') >='20170713' group by ds,server,uid
'''
reg_df = hql_to_df(reg_info_sql)
print reg_df.head(3)
reg_df = reg_df.merge(new_or_old_df, on=['ds','server', ], how='left')
reg_df = reg_df.groupby(['ds','server','dau','delta',]).agg({'uid': lambda g: g.nunique()}).reset_index()
reg_df = reg_df.rename(columns={'uid': 'reg_num',})


result_df = reg_df.merge(pay_df, on=['ds','server','dau','delta',], how='left')
result_df = result_df.merge(coin_df, on=['ds','server','dau','delta',], how='left')
# result_df.to_excel('/Users/kaiqigu/Documents/Superhero/超级英雄-越南-充值对比-1_20170919.xlsx')
# result_df = result_df.merge(new_or_old_df, on=['ds','server','dau','delta',], how='left')
# result_df = result_df.merge(new_or_old_df, on=['ds','server','dau','delta',], how='left')
'''
--------------------------------------
'''

#滚服数据   滚服新增-充值人数-充值金额
is_not_gunfu_sql = '''
        select t2.account,t2.uid,reverse(substring(reverse(t2.uid), 8)) AS server,t2.reg_ds,t2.rank,case when t2.rank=1 then 'fisrt' else 'duokai' end as is_not_duokai from(
            select t1.account,row_number() over( partition by  t1.account  order by min(t1.reg_ds)  ASC ) as rank,t1.uid,t1.reg_ds from (
                  select account,uid,regexp_replace(substring(create_time,1,10),'-','') as reg_ds from mid_info_all where ds='20170918' and regexp_replace(substring(create_time,1,10),'-','') >='20170713'  group by account,uid,reg_ds
            )t1
            where account != ''
            group by t1.account,t1.uid,t1.reg_ds
            order by t1.account,t1.uid,t1.reg_ds
        )t2
        group by t2.account,t2.uid,server,t2.reg_ds,t2.rank,is_not_duokai
    '''
is_not_gunfu_df = hql_to_df(is_not_gunfu_sql)
print is_not_gunfu_df.head()

# 付费
is_gun_pay_sql = '''
    select ds,uid,reverse(substring(reverse(uid),8))as server,sum(order_money) as order_money from raw_paylog where ds>='20170713' and platform_2 != 'admin_test' 
    group by ds,uid,server
'''
is_gun_pay_df = hql_to_df(is_gun_pay_sql)
print is_gun_pay_df.head(3)


is_gun_pay_reg_df = is_not_gunfu_df.merge(is_gun_pay_df, on=['uid','server', ], how='left')
is_gun_pay_reg_df = is_gun_pay_reg_df.groupby(['reg_ds','server','is_not_duokai',]).agg({'order_money': lambda g: g.sum(), 'uid': lambda g: g.nunique()}).reset_index()
is_gun_pay_reg_df = is_gun_pay_reg_df.rename(columns={'uid': 'reg_num', 'reg_ds': 'ds', 'order_money': 'reg_order_money', })
# 滚服新增付费人数
is_gun_reg_pay_df = is_gun_pay_df.merge(is_not_gunfu_df, on=['uid','server', ], how='left')
is_gun_reg_pay_df = is_gun_reg_pay_df[is_gun_reg_pay_df.reg_ds==is_gun_reg_pay_df.ds]
is_gun_reg_pay_df = is_gun_reg_pay_df.groupby(['ds','server','is_not_duokai',]).agg({'order_money': lambda g: g.sum(), 'uid': lambda g: g.nunique()}).reset_index()
is_gun_reg_pay_df = is_gun_reg_pay_df.rename(columns={'uid': 'is_not_gun_reg_pay_num', 'order_money': 'is_not_gun_reg_order_money', })

is_gun_pay_df =is_gun_pay_reg_df.merge(is_gun_reg_pay_df, on=['ds','server','is_not_duokai', ], how='left')
print '------'
is_gun_pay_df = is_gun_pay_df.merge(new_or_old_df, on=['ds','server', ], how='left')
# is_gun_pay_df = is_gun_pay_df.groupby(['ds','server','dau','delta','is_not_duokai',]).agg({'is_not_gun_pay_num': lambda g: g.sum(),'is_not_gun_order_money': lambda g: g.sum()}).reset_index()
# is_gun_pay_df = is_gun_pay_df.rename(columns={'uid': 'reg_num',})

print is_gun_pay_df.head()
is_gun_pay_1_df = (
    is_gun_pay_df.pivot_table('reg_num', ['ds','server','dau','delta',], 'is_not_duokai').reset_index()
        .fillna(0).rename(columns={'first': 'first_reg_num',
                                   'duokai': 'duokai_reg_num',}))
print is_gun_pay_1_df.head()
is_gun_pay_2_df = (
    is_gun_pay_df.pivot_table('reg_order_money', ['ds','server','dau','delta',], 'is_not_duokai').reset_index()
        .fillna(0).rename(columns={'first': 'first_reg_order_money',
                                   'duokai': 'duokai_reg_order_money',}))
print is_gun_pay_2_df.head()
is_gun_pay_3_df = (
    is_gun_pay_df.pivot_table('is_not_gun_reg_pay_num', ['ds','server','dau','delta',], 'is_not_duokai').reset_index()
        .fillna(0).rename(columns={'first': 'first_reg_pay_num',
                                   'duokai': 'duokai_reg_pay_num',}))
print is_gun_pay_3_df.head()
is_gun_pay_4_df = (
    is_gun_pay_df.pivot_table('is_not_gun_reg_order_money', ['ds','server','dau','delta',], 'is_not_duokai').reset_index()
        .fillna(0).rename(columns={'first': 'first_reg_pay_order_money',
                                   'duokai': 'duokai_reg_pay_order_money',}))
print is_gun_pay_4_df.head()

# is_gun_pay_df = is_gun_pay_1_df.merge(is_gun_pay_2_df, on=['ds','server','dau','delta' ], how='left')
is_gun_pay_df = is_gun_pay_1_df.merge(is_gun_pay_3_df, on=['ds','server','dau','delta' ], how='left')
is_gun_pay_df = is_gun_pay_df.merge(is_gun_pay_4_df, on=['ds','server','dau','delta' ], how='left')

# is_gun_pay_df.to_excel('/Users/kaiqigu/Documents/Superhero/超级英雄-越南-充值对比-2_20170919.xlsx')

result = result_df.merge(is_gun_pay_df, on=['ds','server','dau','delta' ], how='left')

result.to_excel('/Users/kaiqigu/Documents/Superhero/超级英雄-越南-充值对比-总_20170919-3.xlsx')
