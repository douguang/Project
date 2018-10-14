#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Hu Chunlong
Description : 每日钻石消费
create_date : 2016.07.18
'''
import settings_dev
from utils import hql_to_df, update_mysql, date_range


def dis_day_coin_spend(date):
    table = 'dis_day_coin_spend'
    sql = '''
    SELECT '{date}' AS ds,
           server,
           vip,
           sum(dau) AS dau,
           sum(free_coin + nvl(charge_coin,0)) AS new_coin,
           sum(free_coin) AS free_get_coin,
           sum(charge_coin) AS pay_get_coin,
           sum(save_coin) AS coin_save,
           (0 - sum(diamond_free_diff + diamond_charge_diff )) AS coin_spend
    FROM
      ( SELECT uid,
               sum(diamond_free_diff) as free_coin
        FROM parse_action_log
        WHERE ds = '{date}'
        AND (diamond_charge_diff  > 0 OR diamond_free_diff > 0)
        GROUP BY uid ) t1
    LEFT OUTER JOIN
      ( SELECT uid,
               sum(nvl(diamond_free_diff,0)) as diamond_free_diff,
               sum(nvl(diamond_charge_diff ,0)) as diamond_charge_diff
       FROM parse_action_log
       WHERE ds = '{date}'
         AND (diamond_charge_diff  < 0
              OR diamond_free_diff < 0)
       GROUP BY uid ) t2 ON t1.uid = t2.uid
    LEFT OUTER JOIN
      ( SELECT uid,
               sum(nvl(order_diamond,0)) + sum(nvl(gift_diamond,0)) AS charge_coin
       FROM raw_paylog
       WHERE ds = '{date}'
         AND platform != 'admin_test' and order_id not like '%testktwwn%'
       GROUP BY uid ) t3 ON t1.uid = t3.uid
    LEFT OUTER JOIN
      ( SELECT uid,
               reverse(substr(reverse(uid), 8)) AS server,
               sum(nvl(diamond_free,0) + nvl(diamond_charge,0)) AS save_coin,
               vip
       FROM parse_info
       WHERE ds = '{date}'
       GROUP BY uid,vip,reverse(substr(reverse(uid), 8))) t4 ON t1.uid = t4.uid
    LEFT OUTER JOIN
      ( SELECT uid,
               count(DISTINCT uid) AS dau
       FROM parse_action_log
       WHERE ds = '{date}'
       GROUP BY uid ) t5 ON t1.uid = t5.uid
   GROUP BY server,vip
   '''.format(**{
        'date': date
    })
    print sql
    df = hql_to_df(sql)
    result_df = df.fillna(0)
    print result_df
    print date, table
    # 更新MySQL
    del_sql = 'delete from {0} where ds="{1}"'.format(table, date)
    update_mysql(table, result_df, del_sql)

# 执行
if __name__ == '__main__':
    for platform in ['crime_empire_pub']:
        settings_dev.set_env(platform)
        for date in date_range('20170909', '20170912'):
            dis_day_coin_spend(date)
        # dis_day_coin_spend('20170110')