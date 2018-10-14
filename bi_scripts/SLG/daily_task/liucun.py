#!/usr/bin/env python  
# encoding: utf-8  

""" 
@author: Andy 
@site:  
@software: PyCharm 
@file: liucun.py 
@time: 18/1/22 下午2:37 
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
        select ds,uid,to_date(reg_time) as reg_ds,to_date(offline_time) as offline_ds from parse_info where ds>='20180119' group by ds,uid,reg_ds,offline_ds
    '''
    print demo_sql
    demo_df = hql_to_df(demo_sql)
    print demo_df.head()
    demo_df = demo_df[['uid','reg_ds','offline_ds']]
    def card_evo_lines():
        for _, row in demo_df.iterrows():
            now = datetime.datetime.strptime(str(row.reg_ds), '%Y-%m-%d')
            end = datetime.datetime.strptime(str(row.offline_ds), '%Y-%m-%d')
            delta = ''
            if now <= end:
                delta = (end - now).days + 1
            else:
                delta = (now - end).days + 1
            # print [row.ds, row.server, row.reg_time,row.dau, delta]
            print [row.uid,row.reg_ds,row.offline_ds,delta,]
            yield [row.uid,row.reg_ds,row.offline_ds,delta,]

    result = pd.DataFrame(card_evo_lines(), columns=['uid', 'reg_ds', 'offline_ds', 'delta',])

    result.to_excel(r'/Users/kaiqigu/Documents/slg/星战帝国-多语言-reg_offline-1_20180122-2.xlsx', index=False)

    plat_result = result.groupby(['reg_ds','delta']).agg({
        'uid': lambda g: g.nunique(),
    }).reset_index().rename(columns={'uid': 'uid_num',})

    plat_result.to_excel(r'/Users/kaiqigu/Documents/slg/星战帝国-多语言-reg_offline-2_20180122-2.xlsx', index=False)

if __name__ == '__main__':
    for platform in ['slg_mul', ]:
        settings_dev.set_env(platform)
        data_reduce()
    print "end"