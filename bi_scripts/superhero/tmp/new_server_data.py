#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Dong Junshuang
Description : 新服数据
'''
import pandas as pd
from utils import date_range, hql_to_df
import settings

settings.set_env('superhero_bi')
name ='g555'
date1 ='20160925'
date2 ='20160929'

vip0_sql = '''
SELECT ds,
       count(DISTINCT uid) vip0
FROM raw_info
WHERE vip_level =0
  AND reverse(substring(reverse(uid), 8)) = '{name}'
  AND ds >='{date1}'
  AND ds <= '{date2}'
GROUP BY ds
ORDER BY ds
'''.format(name=name,date1=date1,date2=date2)
vip_sql = '''
SELECT ds,
       count(DISTINCT uid) vip
FROM raw_info
WHERE vip_level >0
  AND reverse(substring(reverse(uid), 8)) = '{name}'
  AND ds >='{date1}'
  AND ds <= '{date2}'
GROUP BY ds
ORDER BY ds
'''.format(name=name,date1=date1,date2=date2)
act_sql = '''
SELECT ds,
       count(DISTINCT uid) vipn
FROM raw_info
WHERE reverse(substring(reverse(uid), 8)) = '{name}'
  AND ds >='{date1}'
  AND ds <= '{date2}'
GROUP BY ds
ORDER BY ds
'''.format(name=name,date1=date1,date2=date2)
reg_sql = '''
SELECT ds,
       count(DISTINCT uid) reg_num
FROM raw_reg
WHERE ds >='{date1}'
  AND ds <= '{date2}'
  AND reverse(substring(reverse(uid), 8)) = '{name}'
GROUP BY ds
ORDER BY ds
'''.format(name=name,date1=date1,date2=date2)
pay_sql = '''
SELECT ds,
       count(DISTINCT uid) pay_num,
       sum(order_money) sum_money
FROM raw_paylog
WHERE reverse(substring(reverse(uid), 8)) = '{name}'
  AND ds >='{date1}'
  AND ds <= '{date2}'
GROUP BY ds
ORDER BY ds
'''.format(name=name,date1=date1,date2=date2)
pay6_sql = '''
SELECT ds,
       count(DISTINCT uid) AS pay6_num
FROM
  ( SELECT ds,
           uid,
           sum(order_money) sum_money
   FROM raw_paylog
   WHERE reverse(substring(reverse(uid), 8)) = '{name}'
    AND ds >='{date1}'
    AND ds <= '{date2}'
   GROUP BY ds,
            uid)a
WHERE sum_money=6
GROUP BY ds
ORDER BY ds
'''.format(name=name,date1=date1,date2=date2)

act_df = hql_to_df(act_sql)
reg_df = hql_to_df(reg_sql)
vip0_df = hql_to_df(vip0_sql)
vip_df = hql_to_df(vip_sql)
pay_df = hql_to_df(pay_sql)
pay6_df = hql_to_df(pay6_sql)

result =  (act_df
                .merge(reg_df,on=['ds'],how='outer')
                .merge(vip0_df,on=['ds'],how='outer')
                .merge(vip_df,on=['ds'],how='outer')
                .merge(pay_df,on=['ds'],how='outer')
                .merge(pay6_df,on=['ds'],how='outer')
            )
columns = ['ds','vip','vip0','pay_num','sum_money','pay6_num','reg_num','vipn']
result = result[columns]
result.to_excel(r'E:\My_Data_Library\superhero\2016-09-27\{0}.xlsx'.format(name))
