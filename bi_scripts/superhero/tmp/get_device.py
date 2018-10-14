#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
@Author  : Dong Junshuang
@Software: Sublime Text
@Time    : 20170413
Description :  获取ios设备号
'''
import settings_dev
import pandas as pd
from utils import hqls_to_dfs


settings_dev.set_env('superhero_bi')
info_sql = '''
SELECT DISTINCT device
FROM raw_info
WHERE substr(uid,1,1) = 'a'
and device like '%-%'
'''
mid_info_sql = '''
SELECT DISTINCT device
FROM mid_info_all
WHERE ds='20170412'
AND substr(uid,1,1) = 'a'
and device like '%-%'
'''
info_df, mid_info_df = hqls_to_dfs([info_sql, mid_info_sql])
result = pd.concat([info_df, mid_info_df])
result_df = result.drop_duplicates('device')
result_df.to_excel('/Users/kaiqigu/Documents/report/result.xlsx')
