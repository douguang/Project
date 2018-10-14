#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Dong Junshuang
Description : 审计 - 活跃用户数
'''
from utils import hqls_to_dfs
import settings


settings.set_env('superhero_bi')
# 开始日期
date1 = '20160401'
# 截止日期
date2 = '20160630'

reg_sql ='''
SELECT substr(ds,1,6) dt,
       count(uid) reg_user
FROM raw_reg
WHERE ds>='{date1}'
  AND ds<='{date2}'
GROUP BY substr(ds,1,6)
'''.format(date1=date1,date2=date2)
act_sql = '''
SELECT substr(ds,1,6) dt,
       count(uid) act_user
FROM raw_info
WHERE ds>='{date1}'
  AND ds<='{date2}'
GROUP BY substr(ds,1,6)
'''.format(date1=date1,date2=date2)
pay_sql = '''
SELECT substr(ds,1,6) dt,
       count(DISTINCT uid) pay_user,
       sum(order_money) sum_pay
FROM raw_paylog
WHERE ds>='{date1}'
  AND ds<='{date2}'
GROUP BY substr(ds,1,6)
'''.format(date1=date1,date2=date2)
reg_df,act_df,pay_df = hqls_to_dfs([reg_sql,act_sql,pay_sql])

result = (reg_df.merge(act_df,on='dt',how='outer')
                .merge(pay_df,on='dt',how='outer')
                .fillna(0)
                )
result['arppu'] = result['sum_pay']*1.0/result['act_user']
columns = [ 'dt',  'reg_user' , 'act_user',  'pay_user' , 'sum_pay' , 'arppu']
result = result[columns]
result.to_excel('/Users/kaiqigu/Downloads/Excel/shenji/审计-活跃用户数.xlsx')
