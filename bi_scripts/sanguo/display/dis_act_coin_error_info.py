#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Dong Junshuang
Description : 三国 钻石存量异常用户分服数据
create_date : 2016.05.23
'''
import settings_dev
from utils import ds_add, hql_to_df, update_mysql


def dis_act_coin_error_info(date):
    coin_error_sql = '''
SELECT server,
       sum(CASE WHEN coin_num=50001 THEN 1 ELSE 0 END) AS 's50000+' ,
       sum(CASE WHEN coin_num=10000 THEN 1 ELSE 0 END) AS 's5000_10000' ,
       sum(CASE WHEN coin_num=15000 THEN 1 ELSE 0 END) AS 's10001_15000' ,
       sum(CASE WHEN coin_num=20000 THEN 1 ELSE 0 END) AS 's15001_20000' ,
       sum(CASE WHEN coin_num=30000 THEN 1 ELSE 0 END) AS 's20001_30000' ,
       sum(CASE WHEN coin_num=50000 THEN 1 ELSE 0 END) AS 's30001_50000'
FROM
  ( SELECT reverse(substring(reverse(user_id), 8)) AS server,
           user_id,
           vip,
           coin_num
   FROM
      (SELECT user_id,
              vip,
              CASE
                  WHEN coin >=50000 THEN 50001
                  WHEN coin >=5000
                       AND coin <=10000 THEN 10000
                  WHEN coin >=10001
                       AND coin <=15000 THEN 15000
                  WHEN coin >=15001
                       AND coin <=20000 THEN 20000
                  WHEN coin >=20001
                       AND coin <=30000 THEN 30000
                  WHEN coin >=30001
                       AND coin <=50000 THEN 50000
                  ELSE -9999
              END AS coin_num
       FROM mid_info_all
       WHERE ds = '{date}') a
   WHERE coin_num !=-9999 ) t1
LEFT semi JOIN
  ( SELECT user_id
   FROM raw_activeuser
   WHERE ds <= '{date}'
     AND ds >= '{date_ago}' ) t2 ON t1.user_id = t2.user_id
GROUP BY server
'''.format(**{
        'date_ago': ds_add(date, -2),
        'date': date,
    })

    print coin_error_sql
    coin_error_df = hql_to_df(coin_error_sql)
    coin_error_df['ds'] = date
    columns = ['ds', 'server', 's50000+', 's5000_10000', 's10001_15000',
               's15001_20000', 's20001_30000', 's30001_50000']
    coin_error_df = coin_error_df[columns]

    table = 'dis_act_coin_error_info'

    # 更新MySQL表
    del_sql = 'delete from {0} where ds="{1}"'.format(table, date)
    update_mysql(table, coin_error_df, del_sql)

    return coin_error_df


if __name__ == '__main__':
    settings_dev.set_env('sanguo_ks')
    result = dis_act_coin_error_info('20160501')
