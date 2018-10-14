#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Dong Junshuang
Description : 道具
'''
from utils import hql_to_df, ds_add
import pandas as pd
import settings_dev
from pandas import DataFrame

settings_dev.set_env('dancer_tw')

sql = '''
SELECT ds,
       user_id,
       item_dict
FROM parse_info
WHERE ds IN ('20161030',
             '20161031')
'''
tt_df = hql_to_df(sql)

dfs = []
for _, row in tt_df.iterrows():
    # print row['uid']
    # print eval(row['args'])['id'][0]
    if eval(row['item_dict']).has_key('10107'):
        # print row['item_dict']
        data = DataFrame({'ds': row['ds'], 'user_id': row['user_id'],'item': [eval(row['item_dict'])['10107']]})
        dfs.append(data)

result_df = pd.concat(dfs)
result_df.to_excel('/Users/kaiqigu/Documents/Excel/daoju.xlsx')
