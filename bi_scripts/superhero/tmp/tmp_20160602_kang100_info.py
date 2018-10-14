#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Dong Junshuang
Description : 越南 命运装备洗练情况
'''
import settings
from utils import hql_to_df, update_mysql, ds_add,hqls_to_dfs
from pandas import Series,DataFrame
from utils import get_config
import pandas as pd

# settings.set_env('superhero_bi')
settings.set_env('superhero_qiku')

spend_sql = '''
select ds,user_id,coin_num,args from raw_spendlog  where ds in ('20160601','20160602')
'''
print spend_sql
spend_df = hql_to_df(spend_sql)
print 'sql complete'

ds_list = []
uid_list = []
for i in range(len(spend_df)):
    ds_info = spend_df.iloc[i,0]
    uid_info = spend_df.iloc[i,1]
    coin_info = spend_df.iloc[i,2]
    arg_info = spend_df.iloc[i,3]
    info_result = eval(arg_info)
    if info_result.has_key('bless_type'):
        if info_result['bless_type'] == ['2']:
            print info_result['bless_type']
            if coin_info == 100:
                print coin_info
                ds_list.append(ds_info)
                uid_list.append(uid_info)
            else:
                continue
        else:
            continue
    else:
        continue

data = {'ds':ds_list,'uid':uid_list}
data_df = DataFrame(data)

data_df.to_excel('/Users/kaiqigu/Downloads/Excel/qiku.xlsx')
