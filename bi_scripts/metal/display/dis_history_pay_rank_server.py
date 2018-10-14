#!/usr/env/python
# -*- coding:utf-8 -*-
'''
Author      : Hu Chunlong
Description : 各服务器历史累计充值金额排名Top10
'''
import settings_dev
from utils import hql_to_df, update_mysql

def dis_history_pay_rank_server(date):
    total_history_pay_rank_sql = '''
    SELECT server,
           t1.user_id,
           pay_all,
           today_pay,
           last_pay_time,
           vip,
           level,
           reg_time,
           rk
    FROM
      (SELECT reverse(substr(reverse(user_id), 8)) AS server,
              user_id,
              sum(order_money) AS pay_all,
              max(order_time) AS last_pay_time,
              rank() over(PARTITION BY reverse(substr(reverse(user_id), 8))
                          ORDER BY sum(order_money) DESC) rk
       FROM raw_paylog
       WHERE platform_2 != 'admin_test'
         AND ds <= '{date}'
       GROUP BY reverse(substr(reverse(user_id), 8)),
                user_id) t1
    LEFT OUTER JOIN
      (SELECT user_id,
              sum(order_money) AS today_pay
       FROM raw_paylog
       WHERE ds = '{date}'
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
    total_history_pay_rank_df = hql_to_df(total_history_pay_rank_sql)
    total_history_pay_rank_df['ds'] = date

    columns = ['ds', 'server', 'rk', 'user_id', 'today_pay', 'pay_all', 'last_pay_time', 'vip', 'level', 'reg_time']
    result_df = total_history_pay_rank_df[columns].fillna(0)
    print result_df
    # 更新MySQL表
    table = 'dis_history_pay_rank_server'
    del_sql = 'delete from {0} where ds="{1}"'.format(table, date)
    update_mysql(table, result_df, del_sql)

if __name__ == '__main__':
    settings_dev.set_env('metal_test')
    dis_history_pay_rank_server('20160701')
