#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Dong Junshuang
Description :
'''
import settings_dev
from utils import hql_to_df, update_mysql, ds_add, date_range
from pandas import DataFrame
import pandas as pd

settings_dev.set_env('dancer_tw')

act_sql = '''
SELECT ds,
       user_id,
       platform,
       a_tar
FROM mid_actionlog
WHERE ds IN ('20160907',
             '20160908')
and platform = 'androidfb'
'''
act_df = hql_to_df(act_sql)

ds_list,user_id_list,plat_list,appid_list = [],[],[],[]
for _, row in act_df.iterrows():
    args = eval(row['a_tar'])
    if args.has_key('appid'):
        appid = args['appid']
    else:
        appid = ' '
    ds_list.append(row['ds'])
    user_id_list.append(row['user_id'])
    plat_list.append(row['platform'])
    appid_list.append(appid)

result_df = DataFrame({'ds':ds_list,'user_id':user_id_list,'plat':plat_list,'appid':appid_list})

ios_df = result_df.loc[result_df.appid == 'twwnios']
ios_07_df = ios_df.loc[ios_df.ds == '20160907']
ios_08_df = ios_df.loc[ios_df.ds == '20160908']

ios_07_df = ios_07_df.groupby(['ds','user_id']).max().appid.reset_index()
ios_08_df = ios_08_df.groupby(['ds','user_id']).max().appid.reset_index()

result = pd.concat([ios_07_df,ios_08_df])

result.to_excel('/Users/kaiqigu/Downloads/Excel/dt.xlsx')
