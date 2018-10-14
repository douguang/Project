#!/usr/env/python
# -*- coding:utf-8 -*-
'''
Author      : Lan Xuliu
Description : 每日钻石消耗排名Top30
'''
import settings_dev
from utils import hql_to_df, update_mysql


def dis_coins_spend_rank(date):
    table = 'dis_coins_spend_rank'
    print table
    coins_spend_rank_sql = '''
SELECT '{date}' AS ds,
       reverse(substring(reverse(a.user_id), 8)) AS server,
       row_number() over(ORDER BY a.coins DESC) AS rank,
       a.user_id,
       a.coins,
       coalesce(c.pay, 0) AS pay,
       b.vip,
       b.level,
       b.reg_time,
       d.last_pay_time
FROM
  (SELECT user_id,
          sum(coin_num) AS coins
   FROM raw_spendlog
   WHERE ds = '{date}'
   GROUP BY user_id)a
LEFT OUTER JOIN
  (SELECT user_id,
          vip,
          LEVEL,
          reg_time,
          coin
   FROM raw_info
   WHERE ds = '{date}')b ON a.user_id = b.user_id
LEFT OUTER JOIN
  (SELECT user_id,
          sum(order_money) AS pay
   FROM raw_paylog
   WHERE ds = '{date}'
   GROUP BY user_id)c ON a.user_id = c.user_id
LEFT OUTER JOIN
  (SELECT user_id,
          max(ds) AS last_pay_time
   FROM raw_paylog
   WHERE ds <='{date}'
   GROUP BY user_id)d ON a.user_id = d.user_id
ORDER BY rank
LIMIT 30
    '''.format(**{'date': date})
    print coins_spend_rank_sql
    coins_spend_rank_df = hql_to_df(coins_spend_rank_sql)
    del_sql = 'delete from {0} where ds="{1}"'.format(table, date)
    update_mysql(table, coins_spend_rank_df, del_sql)


if __name__ == '__main__':
    settings_dev.set_env('sanguo_ks')
    dis_coins_spend_rank('20160512')
