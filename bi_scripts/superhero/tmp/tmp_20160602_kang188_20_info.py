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

settings.set_env('superhero_bi')
# settings.set_env('superhero_qiku')

spend_sql = '''
select ds,user_id,coin_num,args from raw_spendlog  where ds >= '20160601' and ds <= '20160607' and substr(user_id,1,1) = 'a'
'''
print spend_sql
spend_df = hql_to_df(spend_sql)
print 'sql complete'

ds_list = []
uid_list = []
coin_list = []
ds20_list = []
uid20_list = []
coin20_list = []
for i in range(len(spend_df)):
    ds_info = spend_df.iloc[i,0]
    uid_info = spend_df.iloc[i,1]
    coin_info = spend_df.iloc[i,2]
    if uid_info == 'q142433228' and  coin_info == 50:
        continue
    arg_info = spend_df.iloc[i,3]
    info_result = eval(arg_info)
    if info_result.has_key('bless_type'):
        if info_result['bless_type'] == ['2']:
            print info_result['bless_type']
            if coin_info == 188:
                print uid_info
                print coin_info
                ds_list.append(ds_info)
                uid_list.append(uid_info)
                coin_list.append(coin_info)
            else:
                continue
        elif info_result['bless_type'] == ['1']:
            print info_result['bless_type']
            if coin_info == 20:
                print uid_info
                print coin_info
                ds20_list.append(ds_info)
                uid20_list.append(uid_info)
                coin20_list.append(coin_info)
            else:
                continue
        else:
            continue
    else:
        continue

data = {'ds':ds_list,'uid':uid_list,'coin_num':coin_list}
data_df = DataFrame(data)
data20_dan = {'ds':ds20_list,'uid':uid20_list,'coin_num':coin20_list}
data20_dan_df = DataFrame(data20_dan)

data_result = pd.concat([data_df,data20_dan_df])
del data_result['ds']
data_finalresult = data_result.groupby('uid').sum().reset_index()


data_finalresult.to_excel('/Users/kaiqigu/Downloads/Excel/ios.xlsx')
