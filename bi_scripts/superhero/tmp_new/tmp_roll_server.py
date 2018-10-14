#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Dong Junshuang
Description : 滚服数据
Time        : 2017.06.06
illustration: 日期 、DAU、VIP用户数、总收入、付费人数
注：已排除测试用户
'''
import settings_dev
import pandas as pd
from utils import hqls_to_dfs
from utils import date_range


def get_roll_data(date):
    info_sql = '''
    SELECT a.uid,
           a.account
    FROM
      (SELECT uid,
              account,
              create_time,
              row_number() over(partition BY account
                                ORDER BY create_time DESC) AS rn
       FROM mid_info_all
       WHERE ds ='{date}') a
    JOIN
      ( SELECT account
       FROM mid_info_all
       WHERE ds ='{date}'
       GROUP BY account HAVING count(uid) >1 )b ON a.account = b.account
    AND a.rn = 1
    '''.format(date=date)
    act_sql = '''
    SELECT a.ds,
           a.uid,
           a.vip_level,
           nvl(sum_money,0) AS money,
           CASE WHEN sum_money >0 THEN 1 ELSE 0 END AS is_pay,
           CASE WHEN vip_level >0 THEN 1 ELSE 0 END AS is_vip
    FROM
      (SELECT ds,
              uid,
              vip_level
       FROM raw_info
       WHERE ds ='{date}'
       -- pub的滚服数据
       and substr(uid,1,1) = 'g'
         AND uid NOT IN
           (SELECT uid
            FROM mid_gs)) a
    LEFT OUTER JOIN
      (SELECT uid,
              sum(order_money) sum_money
       FROM raw_paylog
       where ds = '{date}'
       GROUP BY uid)b ON a.uid = b.uid
    '''.format(date=date)
    info_df, act_df = hqls_to_dfs([info_sql, act_sql])
    result = info_df.merge(act_df, on='uid')
    # 滚服数据
    roll_server_result = result.groupby('ds').agg({
        'uid': 'count',
        'is_vip': 'sum',
        'money': 'sum',
        'is_pay': 'sum',
    }).reset_index().rename(columns={'uid': 'roll_dau',
                                     'is_vip': 'roll_vip_num',
                                     'money': 'roll_sum_money',
                                     'is_pay': 'roll_pay_num'})
    column = ['ds', 'roll_dau', 'roll_vip_num', 'roll_sum_money',
              'roll_pay_num']
    result_df = roll_server_result[column]

    return result_df


if __name__ == '__main__':
    settings_dev.set_env('superhero_bi')
    dfs = []
    for date in date_range('20170529', '20170604'):
        print date
        data = get_roll_data(date)
        dfs.append(data)
    result_df = pd.concat(dfs)
    result_df.to_excel('/Users/kaiqigu/Documents/Excel/roll_server_data.xlsx')
