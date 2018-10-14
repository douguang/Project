#!/usr/env/python
# -*- coding:utf-8 -*-
'''
Author      : Hu Chunlong
Description : 服务器每日战力排名Top10
'''
import settings_dev
from utils import hql_to_df, update_mysql

def dis_daily_combat_rank_server(date):
    sql = '''
    SELECT server,
           t1.user_id,
           combat,
           today_pay,
           vip,
           level,
           reg_time,
           last_pay_time,
           rk
    FROM
      (SELECT reverse(substr(reverse(user_id), 8)) AS server,
              user_id,
              combat,
              rank() over(PARTITION BY reverse(substr(reverse(user_id), 8))
                          ORDER BY combat DESC) rk
       FROM raw_info
       WHERE ds = '{date}') t1
    LEFT OUTER JOIN
      (SELECT user_id,
              max(order_time) AS last_pay_time
       FROM raw_paylog
       WHERE ds <= '{date}'
         AND platform_2 != 'admin_test'
       GROUP BY user_id) t2 ON t1.user_id = t2.user_id
    LEFT OUTER JOIN
      (SELECT user_id,
              max(order_money) AS today_pay
       FROM raw_paylog
       WHERE ds = '{date}'
         AND platform_2 != 'admin_test'
       GROUP BY user_id) t3 ON t1.user_id = t3.user_id
    LEFT OUTER JOIN
      (SELECT user_id,
              vip,
              level,
              reg_time
       FROM mid_info_all
       WHERE ds = '{date}') t4 ON t1.user_id = t4.user_id
    WHERE rk <= 10
    ORDER BY rk
    '''.format(**{'date': date})
    mid_df = hql_to_df(sql)
    mid_df['ds'] = date

    columns = ['ds', 'server', 'rk', 'user_id', 'combat', 'today_pay', 'vip', 'level', 'reg_time', 'last_pay_time']
    result_df = mid_df[columns].fillna(0)
    print result_df

    table = 'dis_daily_combat_rank_server'
    del_sql = 'delete from {0} where ds="{1}"'.format(table, date)
    update_mysql(table, result_df, del_sql)

if __name__ == '__main__':
    settings_dev.set_env('metal_test')
    dis_daily_combat_rank_server('20160701')
