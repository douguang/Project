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

settings_dev.set_env('dancer_mul')
date = '20170710'
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
  AND a_typ = 'gacha.get_gacha'
'''.format(date=date)
spend_sql = '''
SELECT user_id,
       goods_type,
       coin_num
FROM raw_spendlog
WHERE ds ='{date}'
AND goods_type = 'gacha.get_gacha'
'''.format(date=date)
pay_sql = '''
SELECT user_id
FROM raw_paylog
WHERE ds = '{date}'
  AND platform_2 <> 'admin_test'
  AND order_id NOT LIKE '%test%'
'''.format(date=date)
info_df, action_df, pay_df, spend_df = hqls_to_dfs(
    [info_sql, action_sql, pay_sql, spend_sql])
spend_df = spend_df.merge(info_df, on='user_id')


def get_id():
    for _, row in action_df.iterrows():
        gacha_sort = int(eval(row.a_tar)['gacha_sort'])
        if gacha_sort in [1, 4]:
            times = 0
        elif gacha_sort == 2:
            times = 1
        elif gacha_sort in [3, 5, 6, 7, 8]:
            times = 10
        yield [row.user_id, row.a_typ, gacha_sort, times]


column = ['user_id', 'a_typ', 'gacha_sort', 'times']
action_result = pd.DataFrame(get_id(), columns=column)
action_result = action_result[action_result.times != 0]
# 登陆人数
act_df = info_df.groupby(['vip', 'server']).count().reset_index().rename(
    columns={'user_id': 'act_num'})
action_result = action_result.merge(info_df, on='user_id')
# 参与次数
attend_df = action_result.groupby(['server', 'vip']).agg({
    'user_id': 'nunique',
    'times': 'sum',
}).reset_index().rename(columns={'user_id': 'uid_num'})
# 消耗钻石
spend_result = spend_df.groupby(['server', 'vip']).sum().reset_index()
# 付费玩家
pay_result = pay_df.merge(action_result, on='user_id')
# 付费玩家参与次数
pay_attend_df = pay_result.groupby(['server', 'vip']).agg({
    'user_id': 'nunique',
    'times': 'sum'
}).reset_index().rename(columns={'user_id': 'pay_uid_num',
                                 'times': 'pay_times'})
result_df = (act_df.merge(attend_df,
                          on=['server', 'vip'],
                          how='outer').merge(pay_attend_df,
                                             on=['server', 'vip'],
                                             how='outer')
             .merge(spend_result,
                    on=['server', 'vip'],
                    how='outer').fillna(0))
result_df['ds'] = date
# times表示抽卡次数
column = ['ds', 'vip', 'server', 'act_num', 'uid_num', 'times', 'coin_num',
          'pay_uid_num', 'pay_times']
result_df = result_df[column]
result_df.to_excel('/Users/kaiqigu/Documents/Excel/limit_hero.xlsx')
