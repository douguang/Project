#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Dong Junshuang
Description : 日常数据(批量补数据)
注：需手动删除更新的数据
'''
from utils import hqls_to_dfs, ds_add, update_mysql, hql_to_df, date_range
import settings
import pandas as pd

if __name__ == '__main__':
    settings.set_env('superhero_qiku')
    start_date = '20160928'
    act_sql = '''
    SELECT distinct ds,
           uid
    FROM raw_action_log
    WHERE ds >= '{0}'
    '''.format(start_date)
    reg_sql = '''
    SELECT  ds,
            uid
       FROM raw_reg
       WHERE  ds = '{0}'
    '''.format(start_date)
    act_df, reg_df = hqls_to_dfs([act_sql, reg_sql])

    # act_df.loc[act_df.ds == start_date]

    d2_df = act_df.loc[act_df.ds == ds_add(start_date,2)]
    d2_df['is_reg'] = d2_df['uid'].isin(reg_df.uid.values)
    d2_reg_df = d2_df[d2_df['is_reg']]
    result = d2_reg_df.count().uid *1.0/reg_df.count().uid
