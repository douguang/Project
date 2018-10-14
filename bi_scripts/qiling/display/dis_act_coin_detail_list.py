#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Dong Junshuang
Description : 三国 三日活跃且钻石存量存量异常的用户详细列表
create_date : 2016.05.24
'''
import settings_dev
import pandas as pd
from utils import ds_add, hql_to_df, update_mysql, get_config, hqls_to_dfs


def dis_act_coin_detail_list(date):
    table = 'dis_act_coin_detail_list'
    print table
    user_sql = '''
SELECT server ,
       user_id ,
       vip ,
       LEVEL ,
       coin ,
       reg_time ,
       act_time ,
       coin_num
FROM
  ( SELECT reverse(substring(reverse(user_id), 8)) AS server,
           user_id,
           vip,
           LEVEL,
           coin,
           reg_time,
           act_time,
           coin_num
   FROM
     ( SELECT user_id,
              vip,
              LEVEL,
              coin,
              reg_time,
              act_time,
              CASE WHEN coin >=50000 THEN 's50000+' WHEN coin >=5000
                   AND coin <=10000 THEN 's5000_10000' WHEN coin >=10001
                   AND coin <=15000 THEN 's10001_15000' WHEN coin >=15001
                   AND coin <=20000 THEN 's15001_20000' WHEN coin >=20001
                   AND coin <=30000 THEN 's20001_30000' WHEN coin >=30001
                   AND coin <=50000 THEN 's30001_50000' ELSE 'None' END AS coin_num
      FROM mid_info_all
      WHERE ds = '{date}' ) a
   WHERE coin_num !='None' ) t1 LEFT semi
JOIN
  ( SELECT user_id
   FROM raw_activeuser
   WHERE ds <= '{date}'
     AND ds >= '{date_in_3days}' ) t2 ON t1.user_id = t2.user_id    '''.format(**{
        'date': date,
        'date_in_3days': ds_add(date, -2),
    })

    # 支付信息
    pay_sql = '''
SELECT user_id ,
       sum(order_money) AS sum_money ,
       max(order_time) AS pay_time
FROM
  ( SELECT user_id ,
           order_money ,
           order_time
   FROM raw_paylog
   WHERE ds <= '{date}'
     AND ds >= '{date_in_3days}'
     AND platform_2 <> 'admin_test' )a
GROUP BY user_id
    '''.format(**{
        'date': date,
        'date_in_3days': ds_add(date, -2),
    })

    print user_sql
    print pay_sql
    user_df, pay_df = hqls_to_dfs([user_sql, pay_sql])
    result = user_df.merge(pay_df, on='user_id', how='left')
    result = result.fillna(0)
    result['ds'] = date
    columns = ['ds', 'coin_num', 'server', 'user_id', 'vip', 'sum_money',
               'coin', 'level', 'reg_time', 'act_time', 'pay_time']
    result = result[columns]



    # 更新MySQL表
    del_sql = 'delete from {0} where ds="{1}"'.format(table, date)
    update_mysql(table, result, del_sql)

    return result


if __name__ == '__main__':
    settings_dev.set_env('sanguo_ks')
    date = '20160426'
    result = dis_act_coin_detail_list(date)
