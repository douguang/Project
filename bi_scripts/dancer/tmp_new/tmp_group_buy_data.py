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
from utils import hql_to_df

settings_dev.set_env('dancer_mul')
date = '20170707'
info_sql = '''
SELECT user_id,
       reverse(substr(reverse(user_id),8)) AS server,
       vip
FROM parse_info
WHERE ds = '{date}'
'''.format(date=date)
spend_sql = '''
SELECT user_id,
       goods_type,
       coin_num
FROM raw_spendlog
WHERE ds ='{date}'
AND goods_type = 'server_group.group_active_buy'
'''.format(date=date)
pay_sql = '''
SELECT user_id
FROM raw_paylog
WHERE ds = '{date}'
  AND platform_2 <> 'admin_test'
  AND order_id NOT LIKE '%test%'
'''.format(date=date)
info_df, spend_df = hqls_to_dfs([info_sql, spend_sql])
pay_df = hql_to_df(pay_sql)
# 登陆人数
act_df = info_df.groupby(['vip', 'server']).count().reset_index().rename(
    columns={'user_id': 'act_num'})
spend_df = spend_df.merge(info_df, on='user_id')
# 参与次数
attend_df = spend_df.groupby(['server', 'vip']).agg({
    'user_id': 'nunique',
    'coin_num': 'sum',
    'goods_type': 'count',
}).reset_index().rename(columns={'user_id': 'uid_num',
                                 'goods_type': 'times'})
# 付费玩家
pay_result = pay_df.merge(spend_df, on='user_id')
# 付费玩家参与次数
pay_attend_df = pay_result.groupby(['server', 'vip']).agg({
    'user_id': 'nunique',
    'goods_type': 'count',
}).reset_index().rename(columns={'user_id': 'pay_uid_num',
                                 'goods_type': 'pay_times'})
result_df = (act_df.merge(attend_df,
                          on=['server', 'vip'],
                          how='outer').merge(pay_attend_df,
                                             on=['server', 'vip'],
                                             how='outer').fillna(0))
result_df['ds'] = date
column = ['ds', 'vip', 'server', 'act_num', 'uid_num', 'times', 'coin_num',
          'pay_uid_num', 'pay_times']
result_df = result_df[column]
result_df.to_excel('/Users/kaiqigu/Documents/Excel/group_buy.xlsx')
