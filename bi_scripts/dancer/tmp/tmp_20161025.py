#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Dong Junshuang
Description :
'''
from utils import hql_to_df, ds_add
import pandas as pd
import settings_dev
from pandas import DataFrame


settings_dev.set_env('dancer_tw')
sql = '''
SELECT ds,
       user_id,
       log_t,
       a_typ,
       a_rst,
       return_code
FROM parse_actionlog
-- WHERE ds>='20160914'
  WHERE ds ='20161015'
  AND user_id = 'tw216220154'
  AND a_typ IN ('cards.new_evolution',
                'cards.sell',
                'cards.step_up')
'''
df = hql_to_df(sql)

df_result = df[df.a_rst <> '[]']
dfs = []
for _,row in df_result.iterrows():
    if len(row['a_rst'])>0:
        data_type = eval(row['a_rst'])
        if isinstance(data_type, list):
            args = list(data_type)
            print row['a_typ']
            for i in args:
                if i['obj'].split('-')[0] == 'Card@30500':
                    print i['after']
                    print i['obj']
                    print i['before']
                    data = DataFrame({'ds': [row['ds']], 'user_id': [row['user_id']],
                        'log_t':[row['log_t']],'a_typ':[row['a_typ']],'before':[i['before']],
                        'obj':[i['obj']],'after':[i['after']]})
                    dfs.append(data)
result_df = pd.concat(dfs)

columns = ['ds','user_id','log_t','a_typ','obj','before','after']
result_df = result_df[columns]
result_df.to_excel('/Users/kaiqigu/Documents/Excel/tw216220154.xlsx')
df_result.to_excel('/Users/kaiqigu/Documents/Excel/tw216220154.xlsx')
