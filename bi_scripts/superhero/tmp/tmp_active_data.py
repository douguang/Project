#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Dong Junshuang
Description : 参与活动的玩家信息
'''
from utils import hql_to_df, ds_add
import pandas as pd
import settings
from pandas import DataFrame

settings.set_env('superhero_vt')
df = pd.read_excel("/Users/kaiqigu/Downloads/Excel/active_data.xlsx")

info_sql = '''
select uid,level,vip_level
from mid_info_all
where ds = '20160917'
'''
info_df = hql_to_df(info_sql)

df = df.rename(columns = {'UID':'uid'})
result_df = df.merge(info_df,on = 'uid',how='left')

result_df.to_excel('/Users/kaiqigu/Downloads/Excel/active_data.xlsx')




