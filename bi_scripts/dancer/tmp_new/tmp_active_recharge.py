#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Dong Junshuang
Description : 限时神将
Time        : 2017.07.13
illustration:
'''
import settings_dev
import pandas as pd
from utils import hqls_to_dfs

id_list = range(167, 187)

settings_dev.set_env('dancer_mul')
date = '20170708'
info_sql = '''
SELECT user_id,
       reverse(substr(reverse(user_id),8)) AS server,
       vip
FROM parse_info
WHERE ds = '{date}'
'''.format(date=date)
action_sql = '''
SELECT user_id,
       a_typ,
       a_tar
FROM parse_actionlog
WHERE ds ='{date}'
  AND a_typ = 'active.server_active_recharge_receive'
'''.format(date=date)
pay_sql = '''
SELECT user_id
FROM raw_paylog
WHERE ds = '{date}'
  AND platform_2 <> 'admin_test'
  AND order_id NOT LIKE '%test%'
'''.format(date=date)
info_df, action_df, pay_df = hqls_to_dfs([info_sql, action_sql, pay_sql])


def get_id():
    for _, row in action_df.iterrows():
        active_id = int(eval(row.a_tar)['active_id'])
        yield [row.user_id, row.a_typ, active_id]


column = ['user_id', 'a_typ', 'active_id']
action_result = pd.DataFrame(get_id(), columns=column)
# 登陆人数
act_df = info_df.groupby(['vip', 'server']).count().reset_index().rename(
    columns={'user_id': 'act_num'})
action_result = action_result.merge(info_df, on='user_id')
# 参与人数
attend_df = action_result.groupby(['server', 'vip', 'active_id']).agg({
    'user_id': 'nunique'
}).reset_index().rename(columns={'user_id': 'uid_num'})
# 付费玩家
pay_result = pay_df.merge(action_result, on='user_id')
# 付费玩家参与人数
pay_attend_df = pay_result.groupby(['server', 'vip', 'active_id']).agg({
    'user_id': 'nunique'
}).reset_index().rename(columns={'user_id': 'pay_uid_num'})
result_df = (act_df.merge(attend_df,
                          on=['server', 'vip'],
                          how='outer').merge(pay_attend_df,
                                             on=['server', 'vip', 'active_id'],
                                             how='outer').fillna(0))
result_df['ds'] = date
result_df = result_df[result_df['active_id'].isin(id_list)]
column = ['ds', 'vip', 'server', 'active_id', 'act_num', 'uid_num',
          'pay_uid_num']
result_df = result_df[column]
result_df.to_excel('/Users/kaiqigu/Documents/Excel/active_recharge.xlsx')
