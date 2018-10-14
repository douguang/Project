#!/usr/env/python
# -*- coding:utf-8 -*-
'''
Author      : Hu Chunlong
Description : 服务器每日充值金额排名Top10
'''
import settings_dev
from utils import hql_to_df, update_mysql

def dis_daily_pay_rank_server(date):
    sql = '''
    SELECT server,
           t1.user_id,
           pay_all,
           today_pay,
           vip,
           level,
           reg_time,
           rk
    FROM
      (SELECT reverse(substr(reverse(user_id), 8)) AS server,
              user_id,
              sum(order_money) AS today_pay,
              rank() over(PARTITION BY reverse(substr(reverse(user_id), 8))
                          ORDER BY sum(order_money) DESC) rk
       FROM raw_paylog
       WHERE platform_2 != 'admin_test'
         AND ds = '{date}'
       GROUP BY reverse(substr(reverse(user_id), 8)),
                user_id) t1
    LEFT OUTER JOIN
      (SELECT user_id,
              sum(order_money) AS pay_all
       FROM raw_paylog
       WHERE ds <= '{date}'
         AND platform_2 != 'admin_test'
       GROUP BY user_id) t2 ON t1.user_id = t2.user_id
    LEFT OUTER JOIN
      (SELECT user_id,
              vip,
              level,
              reg_time
       FROM mid_info_all
       WHERE ds = '{date}') t3 ON t1.user_id = t3.user_id
    WHERE t1.rk <= 10
    ORDER BY t1.rk
    '''.format(**{'date': date})
    mid_df = hql_to_df(sql)
    mid_df['ds'] = date

    columns = ['ds', 'server', 'rk', 'user_id', 'today_pay', 'pay_all', 'vip', 'level', 'reg_time']
    result_df = mid_df[columns].fillna(0)
    print result_df

    table = 'dis_daily_pay_rank_server'
    del_sql = 'delete from {0} where ds="{1}"'.format(table, date)
    update_mysql(table, result_df, del_sql)

if __name__ == '__main__':
    settings_dev.set_env('sanguo_tw')
    dis_daily_pay_rank_server('20160701')
