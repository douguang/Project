#!/usr/bin/env python
# -*- coding: UTF-8 -*-
import settings_dev
from utils import hqls_to_dfs
import pandas as pd


settings_dev.set_env('superhero_bi')
df = pd.read_table("/Users/kaiqigu/Documents/scripts/nginx_log/log_20170207")

info_sql = '''
SELECT uid
FROM raw_info
WHERE ds ='20170207'
  AND regexp_replace(substr(create_time,1,10),'-','') = '20170207'
'''
reg_sql = '''
SELECT uid
FROM raw_reg
WHERE ds ='20170207'
  AND substr(uid,1,1)='g'
'''
info_df, reg_df = hqls_to_dfs([info_sql, reg_sql])

reg_df['is_info'] = reg_df['uid'].isin(info_df.uid.values)
cha_df = reg_df[~reg_df['is_info']]
cha = cha_df.merge(df,on='uid')


