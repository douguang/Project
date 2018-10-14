#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Dong Junshuang
Description : 北冥之灵活动参与情况
'''
from utils import hql_to_df, ds_add
import pandas as pd
import settings_dev
from pandas import DataFrame


settings_dev.set_env('dancer_tw')

gs_list = [
'tw00049382'
,'tw01154702'
,'tw01242651'
,'tw01299343'
,'tw04830830'
,'tw04951783'
,'tw06186815'
,'tw06234331'
,'tw08196949'
,'tw09014611'
,'tw09425502'
,'tw09583563'
,'tw54058426'
,'tw05680792'
,'tw06174068'
,'tw00199670'
,'tw07532132'
,'tw06291078'
,'tw03490521'
,'tw03879592'
,'tw04561893']
gs_df = DataFrame({'user_id':gs_list})


spend_sql = '''
SELECT user_id,
       coin_num
FROM raw_spendlog
WHERE ds = '20160916'
  AND goods_type = 'server_magic_school.open_contract'
  AND reverse(substring(reverse(user_id), 8)) = 'tw0'
'''
spend_df = hql_to_df(spend_sql)

attend_sql = '''
SELECT user_id,
       a_tar
FROM mid_actionlog
WHERE ds = '20160916'
  AND a_typ = 'server_magic_school.contract_exchange'
  and server = 'tw0'
'''
attend_df = hql_to_df(attend_sql)
award_sql = '''
SELECT user_id,
       a_typ,
       a_tar
FROM mid_actionlog
WHERE ds = '20160916'
  AND a_typ LIKE 'server_magic_school.point_reward'
  AND server = 'tw0'
'''
award_df = hql_to_df(award_sql)

spend_df['is_gs'] = spend_df['user_id'].isin(gs_df.user_id.values)
attend_df['is_gs'] = attend_df['user_id'].isin(gs_df.user_id.values)
award_df['is_gs'] = award_df['user_id'].isin(gs_df.user_id.values)

spend_df = spend_df[~spend_df['is_gs']]
attend_df = attend_df[~attend_df['is_gs']]
award_df = award_df[~award_df['is_gs']]

dfs = []
for _, row in attend_df.iterrows():
    # print row['uid']
    # print eval(row['args'])['id'][0]
    data = DataFrame({'user_id':row['user_id'],'which':[eval(row['a_tar'])['which']]})
    dfs.append(data)

result_df = pd.concat(dfs)
# 参与人数
user_df = result_df.drop_duplicates(['which','user_id'])
attend_user_df = user_df.groupby('which').count().reset_index().rename(columns={'user_id':'user_num'})
# 兑换次数
attend_times_df = result_df.groupby('which').count().reset_index().rename(columns={'user_id':'times_num'})
# 北冥之灵的参与情况
result = attend_user_df.merge(attend_times_df,on='which')
# 每个用户参与兑换的次数
result_df['num'] = 1
exchange_times = result_df.groupby(['which','user_id']).count().reset_index()
exchange_times.to_excel('/Users/kaiqigu/Downloads/Excel/exchange_times.xlsx')
result.to_excel('/Users/kaiqigu/Downloads/Excel/exchange.xlsx')

# 消费钻石
spend_result_df = spend_df.groupby('user_id').sum().coin_num.reset_index()
# spend_result = spend_df.merge(result_df,on='user_id',how='left')
# spend_result_data = spend_result.groupby('which').sum().coin_num.reset_index()

spend_result_df.to_excel('/Users/kaiqigu/Downloads/Excel/spend_result_df.xlsx')
# spend_result_data.to_excel('/Users/kaiqigu/Downloads/Excel/spend_result_data.xlsx')

dfs = []
for _, row in award_df.iterrows():
    # print row['uid']
    # print eval(row['args'])['id'][0]
    data = DataFrame({'user_id':row['user_id'],'which':[eval(row['a_tar'])['reward_id']]})
    dfs.append(data)

result_df = pd.concat(dfs)
# 参与人数
user_df = result_df.drop_duplicates(['which','user_id'])
attend_user_df = user_df.groupby('which').count().reset_index().rename(columns={'user_id':'user_num'})
# 参与人数
attend_times_df = result_df.groupby('which').count().reset_index().rename(columns={'user_id':'times_num'})
# 北冥之灵的参与情况
result = attend_user_df.merge(attend_times_df,on='which')
result.to_excel('/Users/kaiqigu/Downloads/Excel/reward.xlsx')

