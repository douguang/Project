#!/usr/env/python
# -*- coding:utf-8 -*-
'''
Author      : Lan Xuliu
Description : 历史累计充值金额排名Top30
'''
import settings_dev
from utils import hql_to_df, update_mysql


def dis_history_pay_rank(date):
    table = 'dis_history_pay_rank'

    daily_pay_rank_sql = '''
SELECT '{date}' AS ds,
       reverse(substring(reverse(a.user_id), 8)) AS server,
       row_number() over(ORDER BY a.pay_total DESC) AS rank,
       a.user_id,
       coalesce(b.pay, 0) AS pay,
       a.pay_total,
       a.last_pay_time,
       c.vip,
       c.level,
       c.reg_time
FROM
  (SELECT user_id,
          sum(order_money) AS pay_total ,
          max(ds) AS last_pay_time
   FROM raw_paylog
   WHERE ds <= '{date}'
   GROUP BY user_id) a
LEFT OUTER JOIN
  (SELECT user_id,
          sum(order_money) AS pay
   FROM raw_paylog
   WHERE ds = '{date}'
   GROUP BY user_id)b ON a.user_id = b.user_id
LEFT OUTER JOIN
  (SELECT user_id,
          vip,
          LEVEL,
          reg_time
   FROM mid_info_all
   WHERE ds = '{date}')c ON a.user_id = c.user_id
ORDER BY rank
LIMIT 30
    '''.format(**{'date': date})
    print daily_pay_rank_sql
    daily_pay_rank_df = hql_to_df(daily_pay_rank_sql)
    del_sql = 'delete from {0} where ds="{1}"'.format(table, date)
    update_mysql(table, daily_pay_rank_df, del_sql)


if __name__ == '__main__':
    settings_dev.set_env('sanguo_ks')
    dis_history_pay_rank('20160501')
