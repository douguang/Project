#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Dong Junshuang
Description : 首冲用户UID
'''
import settings
from utils import hql_to_df, update_mysql, ds_add
import pandas as pd

settings.set_env('superhero_vt')

# sql = '''
# select * from (
# select uid,order_time,
# row_number() over(partition by uid order by order_time ) as rn
# from mid_paylog_all
# where ds ='20160918'
# )a where rn =1
# and order_time >='2016-09-13 23:00:00'
# and order_time <='2016-09-18 10:40:00'
# '''
sql = '''
select *
from (
select uid,min(order_time) order_time
from mid_paylog_all
where ds ='20160918'
group by uid
  ) a
where order_time >='2016-09-13 23:00:00'
and order_time <='2016-09-18 10:40:00'
'''
result = hql_to_df(sql)

df = pd.read_table("/Users/kaiqigu/Downloads/Excel/uid.txt")

df['is_first'] = df['uid'].isin(result.uid.values)
result_df = df[df['is_first']]

# pay_result_df.to_excel('/Users/kaiqigu/Downloads/Excel/pay_coin_cunliang.xlsx')
