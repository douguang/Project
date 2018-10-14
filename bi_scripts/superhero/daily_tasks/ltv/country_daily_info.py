#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
@Time    : 2018/2/8 0008 21:11
@Author  : Andy 
@File    : country_daily_info.py
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
    select ds,uid,regexp_replace(to_date(create_time),'-','') as reg_ds,ip from raw_info where ds>='20180206' group by ds,uid,reg_ds,ip
    '''
    print dau_sql
    dau_df = hql_to_df(dau_sql)
    print dau_df.head()

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
                yield [row.ds,row.uid,row.reg_ds,row.ip, country]
            except:
                country = ''
                yield [row.ds,row.uid,row.reg_ds,row.ip, country]
                pass

    dau_df = pd.DataFrame(ip_lines(), columns=['ds','uid','reg_ds','ip', 'country'])
    print dau_df.head()
    dau_df.to_excel(r'E:\superhero-mul-country-daily_data-1_20180208.xlsx', index=False)
    pay_sql = '''
        select ds,uid,platform_2,level,order_id,regexp_replace(to_date(order_time),'-','') as order_ds,order_time,row_number() over (partition by uid order by order_time) as rn from raw_paylog
        where  ds>='20180206' and platform_2 <> 'admin_test'
        group by ds,uid,platform_2,level,order_id,order_ds,order_time
    '''
    print pay_sql
    pay_df = hql_to_df(pay_sql)
    print pay_df.head()
    pay_df.to_excel(r'E:\superhero-mul-country-daily_data-2_20180208.xlsx', index=False)
    # pay_df = pay_df[['ds', 'uid', 'order_ds', 'order_id', 'rn']].reset_index()


if __name__ == '__main__':
    for platform in ['superhero_mul',]:
        settings_dev.set_env(platform)
        data_reduce()
    print "end"

