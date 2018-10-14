#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Dong Junshuang
Description : 审计 - 充值时段
'''
from utils import hql_to_df
import settings
import pandas as pd

settings.set_env('superhero_bi')
# 开始日期
date1 = '20160401'
# 截止日期
date2 = '20160630'

df = pd.read_excel("/Users/kaiqigu/Downloads/Excel/shenji/shenji_time_ivtl.xlsx")

sql ='''
SELECT ivtl,
       count(DISTINCT uid) pay_user_num,
       count(order_id) pay_times,
       sum(order_money) pay_money from
  (SELECT uid,order_id,order_money,order_time, CASE WHEN tt >=7
   AND tt <12 THEN 'a' WHEN tt >=12
   AND tt <17 THEN 'b' WHEN tt >=17
   AND tt <23 THEN 'c' WHEN tt >=23
   AND tt <= 24 THEN 'd' WHEN tt >=0
   AND tt < 7 THEN 'd' END ivtl
   FROM
     (SELECT uid,order_id,order_money,order_time,hour(order_time) tt
      FROM raw_paylog
      WHERE ds>='20160401'
        AND ds<='20160630' )a )b
GROUP BY ivtl
'''
sql_df = hql_to_df(sql)
result = pd.concat([sql_df,df])
result_df = result.groupby('ivtl').sum().reset_index()
sum_num = result_df.sum().pay_money
result_df['pay_rate'] = result_df['pay_money']*1.0/sum_num
result_df.to_excel('/Users/kaiqigu/Downloads/Excel/shenji/审计-充值时段.xlsx')

