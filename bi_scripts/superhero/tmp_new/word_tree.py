#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Dong Junshuang
Description : 世界之树 - pub
Time        : 2017.06.22
illustration:
世界之树等级: world_tree_new.get_level_reward，sort表示等级
日期  dau 领取奖励1的人数 领取奖励2的人数 3 4 5 6 7 8 9
'''
import settings_dev
import pandas as pd
from utils import hql_to_df
from utils import get_server_days

if __name__ == '__main__':
    settings_dev.set_env('superhero_bi')
    start_date = '20170619'
    end_date = '20170625'
    sql = '''
    SELECT ds,
           uid,
           reverse(substr(reverse(uid), 8)) AS server,
           args
    FROM raw_action_log
    WHERE ds >='{start_date}'
      AND ds <='{end_date}'
      AND action = 'world_tree_new.get_level_reward'
      AND substr(uid,1,1) = 'g'
    '''.format(start_date=start_date, end_date=end_date)
    info_sql = '''
    SELECT ds,
           uid,
           reverse(substr(reverse(uid), 8)) AS server
    FROM raw_info
    WHERE ds >='{start_date}'
      AND ds <='{end_date}'
      AND substr(uid,1,1) = 'g'
    '''.format(start_date=start_date, end_date=end_date)
    df = hql_to_df(sql)
    info_df = hql_to_df(info_sql)

    def get_value():
        for _, row in df.iterrows():
            yield [row.ds, row.uid, row.server, eval(row.args)['sort'][0]]

    column = ['ds', 'uid', 'server', 'sort']
    result = pd.DataFrame(get_value(), columns=column)
    result = (result.groupby(['ds', 'uid']).max().reset_index().groupby(
        ['ds', 'sort']).uid.count().reset_index().pivot_table(
            'uid', ['ds'], 'sort').reset_index().fillna(0))
    # 获取开服天数 - 截止到end_date
    server_df = get_server_days(end_date)
    old_server = server_df[server_df.days > 7]['server'].tolist()
    # 老服的DAU
    dau_df = (info_df[info_df['server'].isin(old_server)].groupby(
        'ds').uid.count().reset_index().rename(columns={'uid': 'dau'}))
    result_df = dau_df.merge(result, on='ds')

    result_df.to_excel('/Users/kaiqigu/Documents/Excel/world_tree.xlsx')

