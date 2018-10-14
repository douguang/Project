#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Hu Chunlong
Description : 每日钻石消费
create_date : 2016.07.18
'''
import settings_dev
from utils import hql_to_df, update_mysql, date_range
from dancer.cfg import zichong_uids

zichong_uids = str(tuple(zichong_uids))

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
           (0 - sum(freemoney_diff + money_diff)) AS coin_spend
    FROM
      ( SELECT user_id,
               sum(freemoney_diff) as free_coin
        FROM parse_actionlog
        WHERE ds = '{date}' and user_id not in {zichong_uids}
        AND (money_diff > 0 OR freemoney_diff > 0)
        GROUP BY user_id ) t1
    LEFT OUTER JOIN
      ( SELECT user_id,
               sum(nvl(freemoney_diff,0)) as freemoney_diff,
               sum(nvl(money_diff,0)) as money_diff
       FROM parse_actionlog
       WHERE ds = '{date}'
         AND (money_diff < 0
              OR freemoney_diff < 0)
       GROUP BY user_id ) t2 ON t1.user_id = t2.user_id
    LEFT OUTER JOIN
      ( SELECT user_id,
               sum(nvl(order_coin,0)) + sum(nvl(gift_coin,0)) AS charge_coin
       FROM raw_paylog
       WHERE ds = '{date}'
         AND platform_2 != 'admin_test' and order_id not like '%testktwwn%'
       GROUP BY user_id ) t3 ON t1.user_id = t3.user_id
    LEFT OUTER JOIN
      ( SELECT user_id,
               reverse(substr(reverse(user_id), 8)) AS server,
               sum(nvl(free_coin,0) + nvl(charge_coin,0)) AS save_coin,
               vip
       FROM parse_info
       WHERE ds = '{date}'
       GROUP BY user_id,vip,reverse(substr(reverse(user_id), 8))) t4 ON t1.user_id = t4.user_id
    LEFT OUTER JOIN
      ( SELECT user_id,
               count(DISTINCT user_id) AS dau
       FROM parse_actionlog
       WHERE ds = '{date}'
       GROUP BY user_id ) t5 ON t1.user_id = t5.user_id
   GROUP BY server,vip
   '''.format(**{
        'date': date, 'zichong_uids': zichong_uids
    })

    df = hql_to_df(sql)
    df = df.fillna(0)
    result_df = df[df['server'] != 0]
    # print result_df
    print date, table
    # 更新MySQL
    del_sql = 'delete from {0} where ds="{1}"'.format(table, date)
    update_mysql(table, result_df, del_sql)

# 执行
if __name__ == '__main__':
    for platform in ['dancer_tw', 'dancer_pub']:
        settings_dev.set_env(platform)
        for date in date_range('20170119', '20170121'):
            dis_day_coin_spend(date)
        # dis_day_coin_spend('20170110')