#!/usr/env/python
# -*- coding:utf-8 -*-
'''
Author      : Lan Xuliu
Description : 每日战斗力排名Top30
'''
import settings_dev
from utils import hql_to_df, update_mysql


def dis_daily_combat_rank(date):
    table = 'dis_daily_combat_rank'

    combat_rest_rank_sql = '''
SELECT '{date}' AS ds,
       reverse(substring(reverse(a.user_id), 8)) AS server,
       row_number() over(ORDER BY a.combat DESC) AS rank,
       a.user_id,
       a.combat,
       coalesce(b.pay, 0) AS pay,
       a.vip,
       a.level,
       a.reg_time,
       c.last_pay_time
FROM
  (SELECT user_id,
          vip,
          LEVEL,
          reg_time,
          combat
   FROM raw_info
   WHERE ds = '{date}')a
LEFT OUTER JOIN
  (SELECT user_id,
          sum(order_money) AS pay
   FROM raw_paylog
   WHERE ds = '{date}'
   GROUP BY user_id)b ON a.user_id = b.user_id
LEFT OUTER JOIN
  (SELECT user_id,
          max(ds) AS last_pay_time
   FROM raw_paylog
   WHERE ds <='{date}'
   GROUP BY user_id)c ON a.user_id = c.user_id
ORDER BY rank
LIMIT 30
    '''.format(**{'date': date})
    print combat_rest_rank_sql
    combat_rest_rank_df = hql_to_df(combat_rest_rank_sql)
    del_sql = 'delete from {0} where ds="{1}"'.format(table, date)
    update_mysql(table, combat_rest_rank_df, del_sql)


if __name__ == '__main__':
    settings_dev.set_env('metal_test')
    dis_daily_combat_rank('20160512')
