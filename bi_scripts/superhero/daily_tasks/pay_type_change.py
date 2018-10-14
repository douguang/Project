#!/usr/bin/env python  
# encoding: utf-8  

""" 
@author: Andy 
@site:  #充值方式改变统计
@software: PyCharm 
@file: pay_type_change.py 
@time: 18/5/5 上午11:28 
"""

import calendar
import settings_dev
from utils import ds_add, hql_to_df, update_mysql, hqls_to_dfs, date_range
import pandas as pd


def data_reduce():
    pay_sql = '''
        select uid from raw_paylog where ds>='20180301' and ds<='20180423' and platform_2 <> 'admin' and platform_2 <> 'admin_test' group by uid
    '''
    print pay_sql
    pay_df = hql_to_df(pay_sql)
    print pay_df.head()
    pay_df.to_excel(r'/Users/kaiqigu/Documents/Superhero/超英-越南-4月23之前的充值用户_20180505.xlsx', index=False)

    info_sql = '''
    select uid,unix_timestamp(fresh_time,'yyyy-MM-dd HH:mm:ss')  as ds from raw_info where ds>='20180301' and ds<='20180423' 
    and uid in ( select uid from raw_paylog where ds>='20180301' and ds<='20180423' and platform_2 <> 'admin' and platform_2 <> 'admin_test' group by uid )
    group by uid,ds order by uid,ds desc
    '''
    print info_sql
    info_df = hql_to_df(info_sql)
    print info_df.head()
    # info_df['ds'] = info_df['ds'].astype()
    info_df['ts_differ'] = 0
    for i in range(len(info_df)):
        if i == len(info_df)-1:
            info_df.iloc[i, 2]=0
        else:
            if info_df.iloc[i, 0]== info_df.iloc[i+1, 0]:
                info_df.iloc[i, 2]=info_df.iloc[i, 1] - info_df.iloc[i+1, 1]
    info_df.to_excel(r'/Users/kaiqigu/Documents/Superhero/超英-越南-4月23之前的活跃用户_20180505.xlsx', index=False)
    result_df = pay_df.merge(info_df, on=['uid', ], how='left')
    result_df.to_excel(r'/Users/kaiqigu/Documents/Superhero/超英-越南-4月23之前的充值用户的活跃情况_20180505.xlsx', index=False)

    pay_sql2 = '''
            select uid from raw_paylog where ds>='20180424'  and platform_2 <> 'admin' and platform_2 <> 'admin_test' group by uid
        '''
    print pay_sql2
    pay_df2 = hql_to_df(pay_sql2)
    print pay_df2.head()
    pay_df2.to_excel(r'/Users/kaiqigu/Documents/Superhero/超英-越南-4月24之后的充值用户_20180505.xlsx', index=False)

if __name__ == '__main__':

    for platform in ['superhero_vt', ]:
        settings_dev.set_env(platform)
        data_reduce()

    print "end"