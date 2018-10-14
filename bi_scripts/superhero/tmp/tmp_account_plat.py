#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Dong Junshuang
Description : 注册用户ltv(account)(指定日期区间)(批量补数据)
注：需手动删除更新的数据
'''
from utils import hqls_to_dfs
from utils import hql_to_df
from utils import ds_add
from utils import date_range
import settings_dev
import pandas as pd
from pandas import DataFrame


def get_num(date):
    sql = '''
    SELECT a.uid,
           b.args,
           a.account
    FROM
      ( SELECT uid, account
       FROM mid_new_account
       WHERE ds ='{date}' ) a
    JOIN
      ( SELECT uid,
               args
       FROM raw_action_log
       WHERE ds ='{date}' )b ON a.uid = b.uid
    '''.format(date=date)
    result = hql_to_df(sql)

    def get_plat():
        for _, row in result.iterrows():
            platform = eval(row['args'])['pt'][0]
            yield [row.account, platform]

    account_df = pd.DataFrame(get_plat(), columns=['account', 'platform'])
    account_result = account_df[account_df.platform == 'android']
    print date, account_result.drop_duplicates('account').count().account


if __name__ == '__main__':
    settings_dev.set_env('superhero_vt')
    end_date = '20170524'
    # start_date = ds_add(end_date, -6)
    for date in date_range(ds_add(end_date, -6), end_date):
        get_num(date)
