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

# 充值区间
ranges = [5, 6, 30, 50, 100, 300, 500, 1000, 1500, 2000, 999999]

if __name__ == '__main__':
    settings_dev.set_env('dancer_mul')
    assist_sql = '''
    SELECT a.ds,
           a.user_id,
           a.order_money,
           b.regist_ip
    FROM
      (SELECT ds,
              user_id,
              sum(order_money) AS order_money
       FROM raw_paylog
       WHERE platform_2 <> 'admin_test'
         AND order_id NOT LIKE '%test%'
       GROUP BY ds,
                user_id)a
    JOIN
      (SELECT user_id,
              max(regist_ip) AS regist_ip
       FROM parse_info
       GROUP BY user_id )b ON a.user_id = b.user_id
    '''
    assist_df = hql_to_df(assist_sql)

    IP.load("/Users/kaiqigu/Documents/scripts/bi_scripts/tinyipdata_utf8.dat")

    def ip_lines():
        for _, row in assist_df.iterrows():
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
                yield [row.ds, row.user_id, row.order_money, country]

    column = ['ds', 'user_id', 'order_money', 'country']
    result = pd.DataFrame(ip_lines(), columns=column)
    # 右区间闭合
    result['ranges'] = pd.cut(result.order_money,
                              ranges, right=False).astype('object')

    result_df = (reszhult.groupby(
        ['ds', 'country', 'ranges']).user_id.count().reset_index().rename(
            columns={'user_id': 'user_num'}).pivot_table(
                'user_num', ['ds', 'country'],
                'ranges').reset_index().fillna(0))
    # result_df.to_excel(r'/Users/kaiqigu/Documents/Excel/pay_country_ivtl.xlsx')
    result_df.to_csv('/Users/kaiqigu/Documents/Excel/pay_country_ivtl', sep='|')
