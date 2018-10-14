#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Dong Junshuang
Description : 每周累充5000钻石以上的玩家人数（上周二 至 下周一）
Time        : 2017.06.29
illustration:
'''
import datetime
import settings_dev
import pandas as pd
from utils import ds_add
from utils import date_range
from utils import hql_to_df

if __name__ == '__main__':
    settings_dev.set_env('superhero_vt')

    dfs = []
    for s_date in date_range('20170501', '20170627'):
        if datetime.datetime.strptime(s_date, '%Y%m%d').weekday() == 1:
            e_date = ds_add(s_date, 6)
            if e_date <= '20170627':
                print s_date, e_date
                sql = '''
                SELECT count(uid)
                FROM
                  (SELECT uid,
                          sum(order_coin) AS sum_money
                   FROM raw_paylog
                   WHERE ds >='{s_date}'
                     AND ds <='{e_date}'
                     AND platform_2 <> 'admin_test'
                   GROUP BY uid )a
                WHERE sum_money>= 5000
                '''.format(s_date=s_date, e_date=e_date)
                df = hql_to_df(sql)
                df['start_date'] = s_date
                df['end_date'] = e_date
                dfs.append(df)

    result_df = pd.concat(dfs)
    result_df.to_excel('/Users/kaiqigu/Documents/Excel/total_coin.xlsx')

