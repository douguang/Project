#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Hu Chunlong
Description : 每日战力排行榜(全服)
Database    : dancer_ks
'''
import settings_dev
from utils import hql_to_df, update_mysql, date_range
from superhero2.cfg import zichong_uids

zichong_uids = str(tuple(zichong_uids))


def dis_daily_combat_rank(date):
    server_start_date = settings_dev.start_date.strftime('%Y%m%d')
    sql = '''
    SELECT '{date}' AS ds,
           server,
           rank,
           t1.uid,
           combat,
           pay,
           vip,
           LEVEL,
           reg_time,
           last_pay_time
    FROM
      (SELECT uid,
              name,
              reverse(substr(reverse(uid), 8)) AS server,
              combat,
              LEVEL,
              vip,
              (diamond_free + diamond_charge) AS coin,
              reg_time,
              Row_Number() OVER (
                                 ORDER BY combat DESC) rank
       FROM mid_info_all
       WHERE ds = '{date}'
         AND uid NOT IN {zichong_uids}) t1
    LEFT OUTER JOIN
      (SELECT uid,
              sum(order_rmb) AS pay
       FROM raw_paylog
       WHERE ds = '{date}'
         AND platform<>'admin_test'
         AND order_id NOT LIKE '%test%'
       GROUP BY uid) t2 ON t1.uid = t2.uid
    LEFT OUTER JOIN
      (SELECT uid,
              max(to_date(order_time)) AS last_pay_time
       FROM raw_paylog
       WHERE ds <= '{date}'
         AND ds >= '{server_start_date}'
         AND platform<>'admin_test'
         AND order_id NOT LIKE '%test%'
       GROUP BY uid) t3 ON t1.uid = t3.uid
    WHERE rank <= 500
    ORDER BY rank
    '''.format(date=date,
               server_start_date=server_start_date,
               zichong_uids=zichong_uids)
    # print sql
    df = hql_to_df(sql)
    df = df.fillna(0)
    # print df
    # 更新MySQL
    table = 'dis_daily_combat_rank'
    print date, table
    del_sql = 'delete from {0} where ds="{1}"'.format(table, date)
    update_mysql(table, df, del_sql)


# 执行
if __name__ == '__main__':
    for platform in ('superhero2_tw', ):
        settings_dev.set_env(platform)
        for date in date_range('20171101', '20171105'):
            dis_daily_combat_rank(date)
