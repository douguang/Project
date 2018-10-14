#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Dong Junshuang
Description : 武娘多语言 - 分国家充值档次分布
Time        :
illustration:
'''
import settings_dev
import pandas as pd
from utils import hql_to_df
from ipip import IP

settings_dev.set_env('dancer_mul')
pay_sql = '''
SELECT a.user_id,
       a.sum_money,
       a.pay_tp,
       b.level,
       b.vip,
       b.regist_ip
FROM
  (SELECT user_id,
          sum(order_money) AS sum_money,
          pay_tp
   FROM raw_paylog
   WHERE ds = '20170713'
     AND pay_tp IN (4,
                    5,
                    6)
     AND platform_2 <> 'admin_test'
     AND order_id NOT LIKE '%test%'
   GROUP BY user_id,
            pay_tp)a
JOIN
  (SELECT user_id,
          vip,
          level,
          regist_ip
   FROM parse_info
   WHERE ds ='20170713') b ON a.user_id = b.user_id
'''
pay_df = hql_to_df(pay_sql)

IP.load("/Users/kaiqigu/Documents/scripts/bi_scripts/tinyipdata_utf8.dat")


def ip_lines():
    for _, row in pay_df.iterrows():
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
            yield [row.user_id, country, row.sum_money, row.pay_tp, row.level,
                   row.vip]


column = ['user_id', 'country', 'sum_money', 'pay_tp', 'level', 'vip']
result = pd.DataFrame(ip_lines(), columns=column)
result.to_csv('/Users/kaiqigu/Documents/Excel/mol_country',
              sep='|')
