#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Dong Junshuang
Description :
'''
from utils import hql_to_df, get_server_list, ds_add, date_range, format_dates, update_mysql
import pandas as pd
import datetime
import settings
from pandas import DataFrame


settings.set_env('superhero_bi')
sql = '''
SELECT uid,
       act_time,
       zhongshendian
FROM raw_action_log
WHERE action ='notify.read'
  AND substring(uid,1,1)='a'
  AND ds='20160628'
  AND zhongshendian IS NOT NULL
'''
tt_df = hql_to_df(sql)
tt_df = tt_df[tt_df['act_time'] <= '2016-06-28 12:00:00']
aa = tt_df.groupby(['uid','zhongshendian']).count().reset_index().rename(columns={'act_time':'r1'})
bb = tt_df.drop_duplicates(['uid','zhongshendian'])
cc = bb.groupby(['uid','zhongshendian']).count().reset_index().rename(columns={'act_time':'r2'})
dd = aa.merge(cc,on=['uid','zhongshendian'])
dd['cha'] = dd['r1'] - dd['r2']
result = dd[dd['cha'] != 0]

tt_df['uid_zhong'] = tt_df['uid'] + tt_df['zhongshendian']
result['uid_zhong'] = result['uid'] + result['zhongshendian']
tt_df['is_copy'] = tt_df['uid_zhong'].isin(result.uid_zhong.values)
mm = tt_df[tt_df['is_copy']]
del mm['uid_zhong']
del mm['is_copy']

uid_list,act_time_list,reward_name_list,reward_list = [],[],[],[]
for j in range(len(mm)):
    uid = mm.iloc[j,0]
    act_time = mm.iloc[j,1]
    args = mm.iloc[j,2]
    for i in eval(args).values()[0].keys():
        if eval(args).values()[0][i] != 0 and eval(args).values()[0][i] != []:
            print i
            print eval(args).values()[0][i]
            uid_list.append(uid)
            act_time_list.append(act_time)
            reward_name_list.append(i)
            reward_list.append(eval(args).values()[0][i])
data = DataFrame({'uid':uid_list,'act_time':act_time_list,'reward_name':reward_name_list,'reward':reward_list})
data = data.sort_index(by=['uid','act_time'])
column = ['uid','act_time','reward_name','reward']
data = data[column]

data.to_excel('/Users/kaiqigu/Downloads/Excel/email.xlsx')
