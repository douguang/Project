#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Dong Junshuang
Description : kang
'''
import settings
from utils import hql_to_df, update_mysql, ds_add,hqls_to_dfs
from pandas import Series,DataFrame
from utils import get_config
import pandas as pd

settings.set_env('superhero_qiku')

info_sql = '''
select ds,uid,args from raw_action_log where uid <> '-10240' and  ds in ('20160601','20160602')
'''
print info_sql
info_df = hql_to_df(info_sql)
print 'sql complete'

ds_list = []
uid_list = []
for i in range(len(info_df)):
    ds_info = info_df.iloc[i,0]
    uid_info = info_df.iloc[i,1]
    arg_info = info_df.iloc[i,2]
    info_result = eval(arg_info)
    if info_result.has_key('rewards'):
        print uid_info
        if info_result['rewards'] == ['7_1_3']:
            print info_result['rewards']
            ds_list.append(ds_info)
            uid_list.append(uid_info)
        else:
            continue
    else:
            continue

data = {'ds':ds_list,'uid':uid_list}
data_df = DataFrame(data)


data_df.to_excel('/Users/kaiqigu/Downloads/Excel/qikukang.xlsx')
