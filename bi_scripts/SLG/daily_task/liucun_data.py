#!/usr/bin/env python  
# encoding: utf-8  

""" 
@author: Andy 
@site:  留存数据
@software: PyCharm 
@file: liucun_data.py 
@time: 18/1/22 上午10:11 
"""

from utils import hqls_to_dfs, get_rank, hql_to_df, date_range
import pandas as pd
from utils import ds_delta
import settings_dev
from pandas import DataFrame
import time
import datetime


def data_reduce():
    demo_sql = '''
        select account,uid,regexp_replace(to_date(reg_time),'-','') as reg_ds,app_id from mid_info_all where ds='20180120' group by account,uid,reg_ds,app_id
    '''
    print demo_sql
    demo_df = hql_to_df(demo_sql)
    print demo_df.head()

    dau_info = '''
        select ds,account,uid from parse_info where ds>='20180119' and to_date(reg_time) >= '2018-01-19' group by ds,account,uid
    '''
    print dau_info
    dau_df = hql_to_df(dau_info)
    print dau_df.head()

    # result = dau_info.merge(demo_df, on=['account','user_id',], how='left')
    result = dau_df.merge(demo_df, on=['account', 'uid'], how='left')
    print '----'
    print result.head()
    def card_evo_lines():
        for _, row in result.iterrows():
            now = datetime.datetime.strptime(str(row.reg_ds), '%Y%m%d')
            end = datetime.datetime.strptime(str(row.ds), '%Y%m%d')
            delta = ''
            if now <= end:
                delta = (end - now).days + 1
            else:
                delta = (now - end).days + 1
            # print [row.ds, row.server, row.reg_time,row.dau, delta]
            print [row.ds,row.reg_ds,delta,row.account, row.uid,row.app_id,]
            yield [row.ds,row.reg_ds,delta,row.account, row.uid,row.app_id,]

    result = pd.DataFrame(card_evo_lines(), columns=['ds', 'reg_ds', 'delta', 'account', 'uid','app_id' ])

    result_df = result.groupby(['ds','delta']).agg({
        'account': lambda g: g.nunique(),
    }).reset_index().rename(columns={'account': 'liucun_num',})

    result_df.to_excel(r'/Users/kaiqigu/Documents/slg/星战帝国-多语言-日常数据留存-1_20180122-2.xlsx', index=False)

    plat_result = result.groupby(['ds', 'app_id','delta']).agg({
        'account': lambda g: g.nunique(),
    }).reset_index().rename(columns={'account': 'liucun_num',})

    plat_result.to_excel(r'/Users/kaiqigu/Documents/slg/星战帝国-多语言-日常数据留存-2_20180122-2.xlsx', index=False)

if __name__ == '__main__':
    for platform in ['slg_mul', ]:
        settings_dev.set_env(platform)
        data_reduce()
    print "end"