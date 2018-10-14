#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
8、9  导入玩家为普通游戏玩家     10、11日导入玩家为二次元用户，运用数据进行对比
日常运营数据：  8、9日进入玩家为一组，10、11日进入玩家为一组，需要新用户、DAU、充值人数、收入、付费率、ARPU、ARPPU
LTV数据：   ltv数据  数据后台只有3日，需要进一步细化天数即可
'''
import settings_dev
from utils import hql_to_df, update_mysql, ds_add, date_range
from pandas import DataFrame
import pandas as pd

def reg_card_equip():
    sql = '''
    SELECT t1.ds,
           sum(dau) AS dau,
           sum(pay_num) AS pay_num,
           sum(pay_rmb) AS pay_rmb
    FROM
      ( SELECT ds,
               user_id,
               count(DISTINCT user_id) AS dau
       FROM parse_info
       WHERE ds >= '20160810'
         AND to_date(reg_time) IN ('2016-08-10',
                                   '2016-08-11')
       GROUP BY ds,
                user_id ) t1
    LEFT OUTER JOIN
      ( SELECT ds,
               user_id,
               count(DISTINCT user_id) AS pay_num,
               sum(order_money) AS pay_rmb
       FROM raw_paylog
       WHERE platform_2 != 'admin_test'
       GROUP BY ds,
                user_id ) t2 ON t1.user_id = t2.user_id
    AND t1.ds = t2.ds
    GROUP BY t1.ds
    ORDER BY t1.ds'''
    df = hql_to_df(sql)
    df['pay_rate'] = df['pay_num'] / df['dau']
    df['ARPU'] = df['pay_rmb'] / df['dau']
    df['ARPPU'] = df['pay_rmb'] / df['pay_num']
    print df
if __name__ == '__main__':
    settings_dev.set_env('dancer_tw')
    reg_card_equip()
