#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Zhang Yongchen
Description : 武娘国服 华为渠道 留存判断 高晓涵
'''
from utils import hql_to_df
import pandas as pd
import settings_dev
from pandas import DataFrame

settings_dev.set_env('dancer_pub')

reg_sql = '''
    select account, min(to_date(reg_time)) as regtime from mid_info_all where to_date(reg_time)>='2016-11-19' and account like '%huawei_%'
    and ds='20161229' and account not in (select account from mid_info_all where ds='20161118' and account like '%huawei_%') group by account
'''
print reg_sql
reg_df = hql_to_df(reg_sql)
print reg_df.head(10)


ds_sql = '''
    select account, ds from parse_info where ds>='20161119' and to_date(reg_time)>='2016-11-19' and account like '%huawei_%' group by account, ds
'''
print ds_sql
ds_df = hql_to_df(ds_sql)
print ds_df.head(10)

df = reg_df.merge(ds_df, on='account', how='right')
print df.head(10)

df.to_excel('/home/kaiqigu/Downloads/gaoxiaohan.xlsx',index=False)
