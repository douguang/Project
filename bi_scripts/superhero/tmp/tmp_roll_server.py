#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Dong Junshuang
Description : pub 滚服数据
create_date : 2016.08.29
'''
import settings
import pandas as pd
from utils import ds_add, hql_to_df, update_mysql, get_config, hqls_to_dfs
import numpy as np

if __name__ == '__main__':
    settings.set_env('superhero_bi')
    date = '20160826'
    info_sql ='''
    SELECT reverse(substr(reverse(uid), 8)) AS server,
           uid,
           account
    FROM
      (SELECT uid,
              account
       FROM raw_info
       WHERE ds='20160828')a LEFT semi
    JOIN
      (SELECT uid
       FROM raw_reg
       WHERE ds='20160828') b ON a.uid = b.uid
    '''
    info_ago_sql = '''
    SELECT reverse(substr(reverse(uid), 8)) AS server,
           uid,
           account
    FROM mid_info_all
    WHERE ds='20160827'
    '''
    info_df = hql_to_df(info_sql)
    info_ago_df = hql_to_df(info_ago_sql)

    info_result_df = (info_df.loc[(info_df.server >= 'g511') &
     (info_df.server <= 'g541') & (info_df.server != 'g52')
     & (info_df.server != 'g53')& (info_df.server != 'g54')])

    # 新增玩家
    new_user_df = (info_result_df
                        .groupby('server')
                        .count()
                        .uid
                        .reset_index()
                        .rename(columns={'uid':'new_user'}))

    info_result_df['is_roll'] = info_result_df['account'].isin(info_ago_df.account.values)
    roll_df = info_result_df[info_result_df['is_roll']]

    # 滚服玩家
    roll_user_df = (roll_df
                        .groupby('server')
                        .count()
                        .uid
                        .reset_index()
                        .rename(columns={'uid':'roll_user'}))

    result_df = new_user_df.merge(roll_user_df,on='server',how='left')
    result_df['rate'] = result_df['roll_user']*1.0/result_df['new_user']
    result_df = result_df.fillna(0)
    print result_df

    # result_df.to_excel('/Users/kaiqigu/Downloads/Excel/roll_server.xlsx')




