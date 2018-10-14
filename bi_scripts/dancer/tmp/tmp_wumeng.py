#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Dong Junshuang
Description : 武盟成员对手工会
'''
from utils import hqls_to_dfs, ds_add
import pandas as pd
import settings_dev
from pandas import DataFrame

settings_dev.set_env('dancer_tw')

duishou_sql = '''
SELECT ds,
       user_id,
       a_tar
FROM parse_actionlog
WHERE ds >='20161009'
  AND ds <='20161020'
  AND a_typ = 'guild_war.battle'
'''
ass_sql = '''
SELECT ds,
       ass_id,
       name,
       player
FROM raw_association
WHERE ds>='20161009'
  AND ds <= '20161020'
'''
duishou_df, ass_df = hqls_to_dfs([duishou_sql, ass_sql])

# 对手数据
dfs = []
for _, row in duishou_df.iterrows():
    # print row['uid']
    # print eval(row['args'])['id'][0]
    data = DataFrame({'ds': row['ds'], 'user_id': row['user_id'],'d_uid': [eval(row['a_tar'])['uid']]})
    dfs.append(data)
result_df = pd.concat(dfs)

# 星辰閣成员
xingchen_df = ass_df.loc[ass_df.ass_id == 'tw581631']
xingchen_dfs = []
for _, row in xingchen_df.iterrows():
    # print row['uid']
    # print eval(row['args'])['id'][0]
    for player in list(eval(row['player'])):
        xingchen_data = DataFrame({'ds': row['ds'], 'ass_id': row['ass_id'], 'name': row['name'],'player': [player]})
        xingchen_dfs.append(xingchen_data)
xingchen_result_df = pd.concat(xingchen_dfs)
xingchen_result_df = xingchen_result_df.rename(columns={'player':'user_id'})

# 与星辰阁对战的玩家
duizhan_df = xingchen_result_df.merge(result_df,on=['ds','user_id'])

# 所有工会的成员
chengyuan_dfs = []
for _, row in ass_df.iterrows():
    # print row['uid']
    # print eval(row['args'])['id'][0]
    for player in list(eval(row['player'])):
        chengyuan_data = DataFrame({'ds': row['ds'], 'ass_id': row['ass_id'], 'name': row['name'],'player': [player]})
        chengyuan_dfs.append(chengyuan_data)
chengyuan_result_df = pd.concat(chengyuan_dfs)

chengyuan_result_df = chengyuan_result_df.rename(columns={'player':'user_id'})

chengyuan_result_df['is_duizhan'] = chengyuan_result_df['user_id'].isin(duizhan_df.d_uid.values)

# df = chengyuan_result_df[chengyuan_result_df['is_duizhan']]

# del chengyuan_result_df['is_duizhan']

# df.to_excel('/Users/kaiqigu/Downloads/Excel/xingchen.xlsx')

duizhan_df = duizhan_df.rename(columns={'ass_id':'xingchen_ass_id','name':'xignchen_name','user_id':'xingcheng_user_id','d_uid':'user_id'})

df = duizhan_df.merge(chengyuan_result_df,on = ['ds','user_id'])

df.to_csv('/Users/kaiqigu/Downloads/Excel/xingchen',index=False)

