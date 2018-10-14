#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Dong Junshuang
Description : 审计 - 玩家充值明细
'''
from utils import hqls_to_dfs,hql_to_df
import settings
import pandas as pd
import numpy as np

settings.set_env('superhero_bi')
# 开始日期
date1 = '20160401'
# 截止日期
date2 = '20160630'

df = pd.read_excel("/Users/kaiqigu/Downloads/Excel/shenji/shenji_detail.xlsx")

pay_sql = '''
SELECT uid,
          platform_2,
          sum(order_money) sum_pay,
          sum(order_coin) sum_coin,
          count(order_id) pay_num,
          max(LEVEL) level,
          count(distinct to_date(order_time)) pay_day_num,
          min(order_time) first_order_time,
          max(order_time) last_order_time
   FROM raw_paylog
   WHERE ds>='{date1}'
     AND ds<='{date2}'
   GROUP BY uid,
            platform_2
'''.format(date1=date1,date2=date2)
info_sql ='''
SELECT uid,
       platform_2,
       min(create_time) create_time,
       max(fresh_time) last_login_time,
       sum(zuanshi) day_coin_num
FROM mid_info_all
WHERE ds = '{date2}'
GROUP BY uid,
         platform_2
'''.format(date2=date2)
act_sql = '''
SELECT uid,
          platform_2,
          count(ds) login_day_num
   FROM raw_act
   WHERE ds>='{date1}'
     AND ds<='{date2}'
   GROUP BY uid,
            platform_2
'''.format(date1=date1,date2=date2)

pay_df,info_df = hqls_to_dfs([pay_sql,info_sql])
act_df = hql_to_df(act_sql)
result = (pay_df.merge(info_df,on=['uid','platform_2'],how='outer')
                .merge(act_df,on=['uid','platform_2'],how='left')
                )
result['is_3000'] = result['uid'].isin(df.uid.values)
data = result[result['is_3000']]
data = data.rename(columns={'LEVEL':'level'})
df = df.rename(columns={'LEVEL':'level'})

result_df = pd.concat([df,data])
# tt = result_df.groupby(['uid','platform_2']).sum().reset_index()
tt = result_df.groupby(['uid','platform_2']).agg({
    'create_time':lambda s: s.min(),
    'day_coin_num':lambda s: s.max(),
    'first_order_time':lambda s: s.min(),
    'last_login_time':lambda s: s.max(),
    'last_order_time':lambda s: s.max(),
    'level':lambda s: s.max(),
    'login_day_num':lambda s: s.sum(),
    'pay_day_num':lambda s: s.sum(),
    'pay_num':lambda s: s.sum(),
    'sum_coin':lambda s: s.sum(),
    'sum_pay':lambda s: s.sum(),
    }).reset_index()
tt = tt.dropna()
tt = tt.sort_values(by='sum_pay',ascending=False)
tt = tt[:3000]

columns = ['uid','sum_pay','pay_num','login_day_num','pay_day_num','create_time','last_login_time','first_order_time','last_order_time','level','sum_coin','day_coin_num','platform_2']
tt = tt[columns]

tt.to_excel('/Users/kaiqigu/Downloads/Excel/shenji/审计-玩家充值明细.xlsx')
