#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Dong Junshuang
Description : 新增用户对应的国家
Time        : 2017.07.10
illustration:
'''
import settings_dev
import pandas as pd
from ipip import IP

from utils import hql_to_df

settings_dev.set_env('dancer_mul')

reg_sql = '''
SELECT user_id,
       max(regist_ip) AS regist_ip
FROM parse_info
WHERE ds <= '20170709'
AND level >=6
GROUP BY user_id
'''
reg_df = hql_to_df(reg_sql)

IP.load("/Users/kaiqigu/Documents/scripts/bi_scripts/tinyipdata_utf8.dat")


def ip_lines():
    for _, row in reg_df.iterrows():
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
            yield [row.user_id, country]


column = ['user_id', 'country']
result = pd.DataFrame(ip_lines(), columns=column)
result_df = result.groupby('country').count().reset_index()
# result_df.to_excel(r'/Users/kaiqigu/Documents/Excel/reg_country.xlsx')
result_df.to_csv('/Users/kaiqigu/Documents/Excel/reg_country', sep='|')
