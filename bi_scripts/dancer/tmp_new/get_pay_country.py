#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Dong Junshuang
Description : 获取指定设备号的付费数据
Time        : 2017.07.03
illustration:
'''
import settings_dev
import pandas as pd
from utils import hql_to_df
from ipip import IP

if __name__ == '__main__':
    settings_dev.set_env('dancer_mul')
    sql = '''
    SELECT b.user_id,
           b.level,
           b.product_id,
           c.regist_ip
    FROM
      (SELECT user_id,
              LEVEL,
              product_id
       FROM
         (SELECT user_id,
                 LEVEL,
                 product_id,
                 row_number() over(partition BY user_id
                                   ORDER BY order_time) AS rn
          FROM raw_paylog
          WHERE platform_2 <> 'admin_test'
            AND order_id NOT LIKE '%test%')a
       WHERE rn =1) b
    JOIN
      (SELECT user_id,
              regist_ip
       FROM mid_info_all
       WHERE ds ='20170703' ) c ON b.user_id = c.user_id
    '''
    df = hql_to_df(sql)
    df['regist_ip'] = df['regist_ip'].astype(basestring)
    IP.load("/Users/kaiqigu/Documents/scripts/bi_scripts/tinyipdata_utf8.dat")

    def ip_lines():
        for _, row in df.iterrows():
            ip = row.regist_ip
            try:
                country = IP.find(ip).strip().encode("utf8")
            except:
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
                yield [row.user_id, row.level, row.product_id, country]

    result = pd.DataFrame(
        ip_lines(),
        columns=['user_id', 'level', 'product_id', 'country'])
    result_df = result.groupby(
        ['country', 'product_id', 'level']).count().reset_index()
    # result_df.to_excel('/Users/kaiqigu/Documents/Excel/pay_country.xlsx')
    result_df.to_csv('/Users/kaiqigu/Documents/Excel/pay_country',
                     sep='|',
                     index=False,
                     header=False)
