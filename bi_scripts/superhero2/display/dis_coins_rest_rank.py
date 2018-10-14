#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Hu Chunlong
Description : 当日钻石存量排行榜(全服)
Database    : dancer_ks
'''
import settings_dev
from utils import hql_to_df, update_mysql, date_range
from superhero2.cfg import zichong_uids

zichong_uids = str(tuple(zichong_uids))


def dis_coins_rest_rank(date):
    server_start_date = settings_dev.start_date.strftime('%Y%m%d')
    sql = '''
    SELECT '{date}' AS ds,
           server,
           rank,
           t1.user_id,
           coin,
           pay,
           vip,
           LEVEL,
           reg_time,
           last_pay_time
    FROM
      (SELECT user_id,
              reverse(substr(reverse(user_id), 8)) AS server,
              LEVEL,
              vip,
              (diamond_free + diamond_charge) AS coin,
              reg_time,
              Row_Number() OVER (
                                 ORDER BY (diamond_free + diamond_charge) DESC) rank
       FROM parse_info
       WHERE ds = '{date}'
         AND user_id NOT IN {zichong_uids}) t1
    LEFT OUTER JOIN
      (SELECT user_id,
              sum(order_rmb) AS pay
       FROM raw_paylog
       WHERE ds = '{date}'
         AND platform<>'admin_test'
         AND order_id NOT LIKE '%test%'
       GROUP BY user_id) t2 ON t1.user_id = t2.user_id
    LEFT OUTER JOIN
      (SELECT user_id,
              max(to_date(order_time)) AS last_pay_time
       FROM raw_paylog
       WHERE ds >= '{server_start_date}'
         AND ds <= '{date}'
         AND platform<>'admin_test'
         AND order_id NOT LIKE '%test%'
       GROUP BY user_id) t3 ON t1.user_id = t3.user_id
    WHERE rank <= 100
    ORDER BY rank
    '''.format(date=date,
               server_start_date=server_start_date,
               zichong_uids=zichong_uids)
    # print sql
    df = hql_to_df(sql)
    df = df.fillna(0)
    # print df
    # 更新MySQL
    table = 'dis_coins_rest_rank'
    print date, table
    del_sql = 'delete from {0} where ds="{1}"'.format(table, date)
    update_mysql(table, df, del_sql)


# 执行
if __name__ == '__main__':
    for platform in ('superhero2_tw',):
        settings_dev.set_env(platform)
        for date in date_range('20171101', '20171105'):
            dis_coins_rest_rank(date)
