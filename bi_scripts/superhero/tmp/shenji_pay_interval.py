#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Dong Junshuang
Description : 审计 - 充值区间
需根据shenji_itvl.xlsx 查出之前的数据，与当前时间段数据进行汇总
'''
from utils import hql_to_df
import settings
import pandas as pd

settings.set_env('superhero_bi')
# 开始日期
date1 = '20160401'
# 截止日期
date2 = '20160630'

df = pd.read_excel("/Users/kaiqigu/Downloads/Excel/shenji/shenji_itvl.xlsx")

pay_sql ='''
SELECT itvl,
       count(uid) user_num,
       sum(pay_times) sum_times,
       sum(sum_pay) sum_pay,
       avg(LEVEL) avg_level,
       avg(pay_times) avg_times
FROM
  (SELECT uid,
          sum_pay,
          LEVEL,
          pay_times,
          CASE
              WHEN sum_pay < 1000 THEN '1000'
              WHEN sum_pay >= 1000
                   AND sum_pay < 3000 THEN '1000_3000'
              WHEN sum_pay >= 3000
                   AND sum_pay < 5000 THEN '3000_5000'
              WHEN sum_pay >= 5000
                   AND sum_pay < 10000 THEN '5000_10000'
              WHEN sum_pay > 10000 THEN '10001'
          END AS itvl
   FROM
     (SELECT uid,
             sum(order_money) sum_pay,
             max(LEVEL) LEVEL,
                        count(order_id) pay_times
      FROM raw_paylog
      WHERE ds>='{date1}'
        AND ds<='{date2}'
      GROUP BY uid)a)b
where itvl is not null
GROUP BY itvl
'''.format(date1=date1,date2=date2)
pay_df = hql_to_df(pay_sql)

ago_df = df.loc[:,['itvl','user_num','sum_times','sum_pay','avg_level','avg_times']]
result = pd.concat([ago_df,pay_df])
result = result.groupby('itvl').sum().reset_index()
result['avg_level'] = result['avg_level']/2
result['avg_times'] = result['avg_times']/2
sum_pay_money = result.sum().sum_pay
result['pay_rate'] = result['sum_pay']*1.0/sum_pay_money

name_df =  df.loc[:,['itvl','name']]
result = result.merge(name_df,on='itvl')

columns = ['name','user_num','sum_times','sum_pay','pay_rate','avg_level','avg_times']
result = result[columns]

result.to_excel('/Users/kaiqigu/Downloads/Excel/shenji/审计-充值区间.xlsx')
