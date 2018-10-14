#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
@Time    : 2018/2/24 0024 17:52
@Author  : Andy 
@File    : paylog_on_country.py
@Software: PyCharm
Description :
'''

import calendar
import settings_dev
from utils import ds_add, hql_to_df, update_mysql, hqls_to_dfs, date_range
import pandas as pd
from pandas import DataFrame
from ipip import IP
import os

def data_reduce():
    dau_sql = '''
        select uid,regexp_replace(to_date(create_time),'-','') as reg_ds,ip from mid_info_all where ds='20180223' group by uid,reg_ds,ip
        '''
    print dau_sql
    dau_df = hql_to_df(dau_sql)
    print dau_df.head()

    pay_sql = '''
            select ds,uid,sum(order_money) as order_money from raw_paylog
            where  ds>='20180206' and platform_2 <> 'admin_test'
            group by ds,uid
        '''
    print pay_sql
    pay_df = hql_to_df(pay_sql)
    print pay_df.head()

    dau_df = pay_df.merge(dau_df, on=['uid', ], how='left')

    dau_df['ip'] = dau_df['ip'].astype(basestring)
    IP.load(os.path.abspath("./tinyipdata_utf8.dat"))
    def ip_lines():
        for _, row in dau_df.iterrows():
            ip = row.ip
            try:
                country = IP.find(ip).strip().encode("utf8")
                if '中国台湾' in country:
                    country = '台湾'
                elif '中国香港' in country:
                    country = '香港'
                elif '中国澳门' in country:
                    country = '澳门'
                elif '中国' in country:
                    country = '中国'
                yield [row.ds,row.uid,row.reg_ds,row.ip, country,row.order_money,]
            except:
                country = ''
                yield [row.ds, row.uid, row.reg_ds, row.ip, country,row.order_money,]

    result_df = pd.DataFrame(ip_lines(), columns=['ds','uid','reg_ds','ip', 'country','order_money'])
    print result_df.head()
    # result_df = result_df.groupby(['ds', 'country', ]).agg(
    #     {'order_money': lambda g: g.sum(), }).reset_index().rename(
    #     columns={'order_money': 'order_money', })
    # print result_df.head()

    result_df.to_excel(r'E:\superhero-mul-country-paylog_data_20180224.xlsx', index=False)

if __name__ == '__main__':
    for platform in ['superhero_mul',]:
        settings_dev.set_env(platform)
        data_reduce()
    print "end"


