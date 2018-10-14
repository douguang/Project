#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Dong Junshuang
Description : 幸运轮盘
Time        : 2017.06.05
illustration:
'''
import settings_dev
import datetime
import pandas as pd
from utils import hqls_to_dfs

settings_dev.set_env('dancer_mul')
date = '20170711'
# 当日23点
end_date = (
    datetime.datetime.strptime(date, '%Y%m%d') + datetime.timedelta(hours=23)
).strftime('%Y-%m-%d %H:%M:%S')
info_sql = '''
SELECT user_id,
       reverse(substr(reverse(user_id),8)) AS server,
       vip
FROM parse_info
WHERE ds = '{date}'
'''.format(date=date)
action_sql = '''
SELECT user_id,
       a_typ
FROM parse_actionlog
WHERE ds ='{date}'
  AND log_t <= '{end_date}'
  AND a_typ IN ('server_roulette.open_roulette',
                'server_roulette.open_roulette10',
                'server_roulette.refresh')
'''.format(date=date, end_date=end_date)
spend_sql = '''
SELECT user_id,
       goods_type,
       coin_num
FROM raw_spendlog
WHERE ds ='{date}'
  AND subtime <= '{end_date}'
  AND goods_type IN ('server_roulette.open_roulette',
                'server_roulette.open_roulette10',
                'server_roulette.refresh')
'''.format(date=date, end_date=end_date)
info_df, spend_df, action_df = hqls_to_dfs([info_sql, spend_sql, action_sql])


def get_action():
    for _, row in action_df.iterrows():
        if row.a_typ == 'server_roulette.refresh':
            fresh_time = 1
            open_time = 0
        elif row.a_typ == 'server_roulette.open_roulette':
            fresh_time = 0
            open_time = 1
        elif row.a_typ == 'server_roulette.open_roulette10':
            fresh_time = 0
            open_time = 10
        yield [row.user_id, row.a_typ, fresh_time, open_time]
# 生成DataFrame
column = ['user_id', 'a_typ', 'fresh_time', 'open_time']
action_df = pd.DataFrame(get_action(), columns=column)
action_df = action_df.merge(info_df, on='user_id')
spend_df = spend_df.merge(info_df, on='user_id')

# 登陆人数
act_df = info_df.groupby(['vip', 'server']).count().reset_index().rename(
    columns={'user_id': 'act_num'})
# 参与次数
attend_df = action_df.groupby(['server', 'vip']).agg({
    'open_time': 'sum',
    'user_id': 'nunique',
    'fresh_time': 'sum',
}).reset_index().rename(columns={'user_id': 'uid_num'})
# 消耗钻石数
spend_result = spend_df.groupby(['server', 'vip']).sum().reset_index()
# 消耗钻石最多的UID
# uid_df = spend_df.groupby(['vip', 'server', 'user_id']).sum().reset_index()
# max_df = uid_df.groupby(['vip', 'server']).coin_num.max().reset_index()
# result = uid_df.merge(max_df, on=['vip', 'server', 'coin_num'])
# del result['coin_num']
# 汇总
result_df = (act_df.merge(attend_df,
                          on=['server', 'vip'],
                          how='outer').merge(spend_result,
                                             on=['server', 'vip'],
                                             how='outer').fillna(0))
             # .merge(result, on=['server', 'vip'],
             #        how='outer').fillna(0))
result_df['ds'] = date
column = ['ds', 'vip', 'server', 'act_num', 'uid_num', 'open_time', 'coin_num',
          'fresh_time']
result_df = result_df[column]
result_df.to_excel('/Users/kaiqigu/Documents/Excel/roulette.xlsx')
# result.to_excel('/Users/kaiqigu/Documents/Excel/roulette_coin.xlsx')






