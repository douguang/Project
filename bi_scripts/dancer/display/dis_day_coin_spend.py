#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Hu Chunlong
Description : 每日钻石消费
Create_date : 2016.07.18
Illustration: Dong Junshuang 2017.05.26日更新，为提升页面打开速度去掉了server
              Dong Junshuang 2017.06.13日优化SQL，为保证各页面的用户数统计一致
'''
import settings_dev
from utils import hql_to_df
from utils import date_range
from utils import update_mysql
from dancer.cfg import zichong_uids

zichong_uids = str(tuple(zichong_uids))


def dis_day_coin_spend(date):
    table = 'dis_day_coin_spend'
    sql = '''
    SELECT '{date}' AS ds,
           vip,
           sum(dau) AS dau,
           sum(free_coin + nvl(charge_coin,0)) AS new_coin,
           sum(free_coin) AS free_get_coin,
           sum(charge_coin) AS pay_get_coin,
           sum(save_coin) AS coin_save,
           (0 - sum(freemoney_diff + money_diff)) AS coin_spend
    FROM
      (SELECT user_id,
              count(DISTINCT user_id) AS dau
       FROM parse_info
       WHERE ds = '{date}'
       and user_id not in {zichong_uids}
       GROUP BY user_id) t5
    LEFT OUTER JOIN
      (SELECT user_id,
              sum(freemoney_diff) AS free_coin
       FROM parse_actionlog
       WHERE ds = '{date}'
         AND user_id NOT IN {zichong_uids}
         AND (money_diff > 0
              OR freemoney_diff > 0)
       GROUP BY user_id) t1 ON t1.user_id = t5.user_id
    LEFT OUTER JOIN
      (SELECT user_id,
              sum(nvl(freemoney_diff,0)) AS freemoney_diff,
              sum(nvl(money_diff,0)) AS money_diff
       FROM parse_actionlog
       WHERE ds = '{date}'
         AND (money_diff < 0
              OR freemoney_diff < 0)
       GROUP BY user_id) t2 ON t5.user_id = t2.user_id
    LEFT OUTER JOIN
      (SELECT user_id,
              sum(nvl(order_coin,0)) + sum(nvl(gift_coin,0)) AS charge_coin
       FROM raw_paylog
       WHERE ds = '{date}'
         AND platform_2 != 'admin_test'
         AND order_id NOT LIKE '%test%'
       GROUP BY user_id) t3 ON t5.user_id = t3.user_id
    LEFT OUTER JOIN
      (SELECT user_id,
              sum(nvl(free_coin,0) + nvl(charge_coin,0)) AS save_coin,
              vip
       FROM parse_info
       WHERE ds = '{date}'
       GROUP BY user_id,
                vip
                ) t4 ON t5.user_id = t4.user_id
    GROUP BY vip
   '''.format(**{
        'date': date,
        'zichong_uids': zichong_uids
    })

    df = hql_to_df(sql)
    result_df = df.fillna(0)
    # print result_df.groupby('ds').sum()
    print date, table
    # 更新MySQL
    del_sql = 'delete from {0} where ds="{1}"'.format(table, date)
    update_mysql(table, result_df, del_sql)


if __name__ == '__main__':
    for platform in ['dancer_pub']:
        settings_dev.set_env(platform)
        for date in date_range('20170612', '20170612'):
            dis_day_coin_spend(date)
        # dis_day_coin_spend('20170110')
