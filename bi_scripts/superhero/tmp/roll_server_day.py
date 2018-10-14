#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Dong Junshuang
Description : pub 滚服数据(单日)
create_date : 2016.09.01
'''
import settings
import pandas as pd
from utils import ds_add, hql_to_df, hqls_to_dfs
import numpy as np

if __name__ == '__main__':
    settings.set_env('superhero_bi')
    date = '20160926'
    server_list = ['g547']
    server_ago_list = ['g546']
    info_sql ='''
    SELECT reverse(substr(reverse(uid), 8)) AS server,
           uid,
           account
    FROM
      (SELECT uid,
              account
       FROM raw_info
       WHERE ds='{date}')a LEFT semi
    JOIN
      (SELECT uid
       FROM raw_reg
       WHERE ds='{date}') b ON a.uid = b.uid
    '''.format(date=date)
    info_ago_sql = '''
    SELECT reverse(substr(reverse(uid), 8)) AS server,
           uid,
           account
    FROM mid_info_all
    WHERE ds='{date_ago}'
    '''.format(date_ago=ds_add(date,-1))
    info_df,info_ago_df = hqls_to_dfs([info_sql,info_ago_sql])

    dfs = []
    for num,server in enumerate(server_list):
        # print num,server
        # print server_ago_list[num]
        info_result_df = info_df.loc[info_df.server == server]
        # 玩家总数
        new_user_df = (info_result_df
                            .groupby('server')
                            .count()
                            .uid
                            .reset_index()
                            .rename(columns={'uid':'user_num'}))

        server_df = info_df.loc[info_df.server == server]
        other_df = info_df.loc[info_df.server != server]
        server_df['is_roll'] = server_df['account'].isin(other_df.account.values)
        server_df = server_df[server_df['is_roll']]
        # 滚服玩家
        roll_user_df = (server_df
                            .groupby('server')
                            .count()
                            .uid
                            .reset_index()
                            .rename(columns={'uid':'roll_user'}))
        # 来自前一个服的滚服玩家
        server_df = info_df.loc[info_df.server == server]
        other_df = info_df.loc[info_df.server == server_ago_list[num]]
        server_df['is_roll'] = server_df['account'].isin(other_df.account.values)
        server_df = server_df[server_df['is_roll']]
        roll_ago_user_df = (server_df
                            .groupby('server')
                            .count()
                            .uid
                            .reset_index()
                            .rename(columns={'uid':'roll_ago_user'}))
        result_df = (new_user_df
                            .merge(roll_user_df,on='server',how='left')
                            .merge(roll_ago_user_df,on='server',how='left'))
        dfs.append(result_df)

    result = pd.concat(dfs)
    result.to_excel('/Users/kaiqigu/Downloads/Excel/roll_server.xlsx')


