#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Dong Junshuang
Description : 获取指定设备号的付费数据
Time        : 2017.07.03
illustration:
'''
import settings_dev
import pandas as pd
from utils import hqls_to_dfs

if __name__ == '__main__':
    settings_dev.set_env('dancer_pub')
    ifda_df = pd.read_excel('/Users/kaiqigu/Documents/Excel/ifda_data.xlsx')
    info_sql = '''
    SELECT user_id,
           device_mark
    FROM mid_info_all
    WHERE ds ='20170705'
    '''
    pay_sql = '''
    SELECT user_id,
           sum(order_money) AS sum_money
    FROM raw_paylog
    WHERE ds >= '20170628'
      AND ds <= '20170705'
    GROUP BY user_id
    '''
    info_df, pay_df = hqls_to_dfs([info_sql, pay_sql])
    result = info_df.merge(ifda_df, on='device_mark').merge(
        pay_df, on='user_id', how='left').fillna(0)
    result_df = result.groupby('device_mark').sum_money.sum().reset_index()
    result_df.to_excel('/Users/kaiqigu/Documents/Excel/ifda_pay.xlsx')
