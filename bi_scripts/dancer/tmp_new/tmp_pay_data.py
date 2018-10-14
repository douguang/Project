#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Dong Junshuang
Description : 充值用户付费构成
Time        : 2017.07.10
illustration: 充值用户UID，国家，注册时间，充值总额，充值构成(即每天充值的金额)
'''
import settings_dev
import pandas as pd
from ipip import IP
from utils import ds_add
from utils import hql_to_df

settings_dev.set_env('dancer_mul')
date = '20170629'
rename_dic = {ds_add(date, i - 1): 'd%d_money' % i for i in range(1, 12)}

pay_sql = '''
SELECT a.ds,
       a.user_id,
       a.order_money,
       b.reg_time,
       b.regist_ip,
       b.level,
       a.product_id
FROM
  (SELECT ds,
          user_id,
          order_money,
          product_id
   FROM raw_paylog
   WHERE platform_2 <> 'admin_test'
     AND order_id NOT LIKE '%test%') a
JOIN
  (SELECT user_id,
          min(reg_time) AS reg_time,
          max(regist_ip) AS regist_ip,
          max(level) as level
   FROM parse_info
   GROUP BY user_id) b ON a.user_id = b.user_id
'''
pay_df = hql_to_df(pay_sql)

# 充值总额 - 主要是计算order_money，其他都是附带字段
total_df = pay_df.groupby('user_id').agg({
    'regist_ip': 'max',
    'reg_time': 'min',
    'order_money': 'sum',
    'level': 'max',
}).reset_index()

IP.load("/Users/kaiqigu/Documents/scripts/bi_scripts/tinyipdata_utf8.dat")


def ip_lines():
    for _, row in total_df.iterrows():
        ip = row.regist_ip
        try:
            country = IP.find(ip).strip().encode("utf8")
        except:
            print ip
            country = '未知国家'
        finally:
            if '中国台湾' in country:
                country = '台湾'
            elif '中国香港' in country:
                country = '香港'
            elif '中国澳门' in country:
                country = '澳门'
            elif '中国' in country:
                country = '中国'
            yield [row.user_id, country, row.reg_time, row.order_money,
                   row.level]


column = ['user_id', 'country', 'reg_time', 'order_money', 'level']
result = pd.DataFrame(ip_lines(), columns=column)

pay_result = (pay_df.groupby(['ds', 'user_id']).sum().reset_index()
              .pivot_table('order_money', ['user_id'], 'ds').reset_index()
              .fillna(0))

product_result = (
    pay_df.groupby(['user_id', 'product_id']).sum().reset_index()
    .pivot_table('order_money', ['user_id'], 'product_id').reset_index()
    .fillna(0))

result_df = result.merge(pay_result, on='user_id').merge(product_result,
                                                         on='user_id')
# result_df.to_excel(r'/Users/kaiqigu/Documents/Excel/pay_data.xlsx')
result_df.to_csv('/Users/kaiqigu/Documents/Excel/pay_data', sep='|')
