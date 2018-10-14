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


settings.set_env('superhero_qiku')
sql = '''
SELECT uid,timestmp,args
FROM raw_action_log
WHERE ds ='20160623'
  AND action = 'cards.super_evolution'
'''
tt_df = hql_to_df(sql)
# tt_df['day'] = tt_df.timestmp.map(lambda s: pd.Period(s))

# for i in len(tt_df):
# open_date = datetime.datetime.fromtimestamp(1466697856).strftime('%Y%m%d %H:%M:%S')
uid_list,mm_list,args_list = [],[],[]
for i in range(len(tt_df)):
    uid = tt_df.iloc[i,0]
    tt = tt_df.iloc[i,1]
    args = tt_df.iloc[i,2]
    args = eval(args)
    aa = datetime.datetime.fromtimestamp(tt).strftime('%Y-%m-%d %H:%M:%S')
    if args.has_key('major'):
        print args['major']
        card = args['major'][0].split('-')[0]
        args_list.append(card)
        uid_list.append(uid)
        mm_list.append(aa)
    else:
        continue
data = DataFrame({'user_id':uid_list,'tt':mm_list,'card':args_list})
ll = data[data['card'] == '33200']

super_step = "select uid,super_step_level from raw_super_step where ds ='20160623' and card_id = 33200 "
super_step_df = hql_to_df(super_step)
super_step_day = "select uid,max(super_step_level) super_step_level from raw_super_step where ds >='20160623' and ds <='20160627' and card_id = 33200 group by uid "
super_step_day_df = hql_to_df(super_step_day)
super_step_df = super_step_df.rename(columns={'uid':'user_id'})
super_step_day_df = super_step_day_df.rename(columns={'uid':'user_id'})
result = ll.groupby(['user_id','card']).count().reset_index()
final_result = result.merge(super_step_df,on = 'user_id',how='left')
aa = final_result.merge(super_step_day_df,on = 'user_id',how='left')

aa.to_excel('/Users/kaiqigu/Downloads/Excel/wuyaowang.xlsx')


