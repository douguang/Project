#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Hu Chunlong
Description : 分接口钻石新增
warn        : Dmp102001 为充值钻石
'''
import settings_dev
import pandas as pd
from utils import update_mysql, hqls_to_dfs

def dis_coin_achieve_api(date):
    paylog_sql = '''
    SELECT '{date}' AS ds,
           server,
           vip,
           'charge_reason' AS reason,
           sum(diff) as diff,
           sum(num) as num,
           sum(diff) / sum(num) as AVG
    FROM
      ( SELECT user_id,
               reverse(substr(reverse(user_id), 8)) AS server,
               sum(order_coin) + sum(gift_coin) AS diff,
               count(DISTINCT user_id) AS num
       FROM raw_paylog
       WHERE ds = '{date}'
         AND platform_2 != 'admin_test'
       GROUP BY user_id,
                reverse(substr(reverse(user_id), 8)) ) t1
    LEFT OUTER JOIN
      ( SELECT user_id,
               vip
       FROM mid_info_all
       WHERE ds = '{date}' ) t2 ON t1.user_id = t2.user_id
    GROUP BY vip,
             server
    '''.format(**{
        'date': date,
    })

    action_sql = '''
    SELECT '{date}' AS ds,
           server,
           vip_level as vip,
           a_typ AS reason,
           sum(CASE WHEN freemoney_diff >= 0 THEN freemoney_diff ELSE 0 END) + sum(CASE WHEN money_diff >= 0 THEN money_diff ELSE 0 END) AS diff,
           count(DISTINCT user_id) AS num,
           CAST ((sum(CASE WHEN freemoney_diff >= 0 THEN freemoney_diff ELSE 0 END) + sum(CASE WHEN money_diff >= 0 THEN money_diff ELSE 0 END)) / count(DISTINCT user_id) AS int) AS AVG
    FROM parse_actionlog
    WHERE ds = '{date}'
      AND a_typ != 'Dmp102001'
      AND (freemoney_diff >= 0
           OR money_diff >= 0)
    GROUP BY a_typ,
             server,
             vip_level
    '''.format(**{
        'date': date,
    })
    paylog_df, action_df = hqls_to_dfs([action_sql, paylog_sql])
    df_old = pd.concat([action_df, paylog_df])
    df = df_old.sort_values(['diff'], ascending=False)
    #print df
    # 更新MySQL表
    table = 'dis_coin_achieve_api'
    print table
    del_sql = 'delete from {0} where ds="{1}"'.format(table, date)
    update_mysql(table, df, del_sql)
    return df
# 执行
if __name__ == '__main__':
    settings_dev.set_env('sanguo_tw')
    dis_coin_achieve_api('20160701')
