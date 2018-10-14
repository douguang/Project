#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Dong Junshuang
Description : 三国流失用户
'''
import settings_dev
from utils import hql_to_df, update_mysql, ds_add


def dis_act_loss_info(excute_date):
    '''流失的定义为一周未登陆，因此若传入的参数为 20160518，实际计算的是 20160511 的流失数据
    '''
    date = ds_add(excute_date, -7)
    table = 'dis_act_loss_info'

    act_loss_sql = '''
SELECT server,
       count(user_id) AS act_sum,
       sum(CASE WHEN vip=0 THEN 1 ELSE 0 END) AS vip0,
       sum(CASE WHEN vip=1 THEN 1 ELSE 0 END) AS vip1,
       sum(CASE WHEN vip=2 THEN 1 ELSE 0 END) AS vip2,
       sum(CASE WHEN vip=3 THEN 1 ELSE 0 END) AS vip3,
       sum(CASE WHEN vip=4 THEN 1 ELSE 0 END) AS vip4,
       sum(CASE WHEN vip=5 THEN 1 ELSE 0 END) AS vip5,
       sum(CASE WHEN vip=6 THEN 1 ELSE 0 END) AS vip6,
       sum(CASE WHEN vip=7 THEN 1 ELSE 0 END) AS vip7,
       sum(CASE WHEN vip=8 THEN 1 ELSE 0 END) AS vip8,
       sum(CASE WHEN vip=9 THEN 1 ELSE 0 END) AS vip9,
       sum(CASE WHEN vip=10 THEN 1 ELSE 0 END) AS vip10,
       sum(CASE WHEN vip=11 THEN 1 ELSE 0 END) AS vip11,
       sum(CASE WHEN vip=12 THEN 1 ELSE 0 END) AS vip12,
       sum(CASE WHEN vip=13 THEN 1 ELSE 0 END) AS vip13,
       sum(CASE WHEN vip=14 THEN 1 ELSE 0 END) AS vip14,
       sum(CASE WHEN vip=15 THEN 1 ELSE 0 END) AS vip15
FROM
  ( SELECT user_id,
           vip,
           reverse(substring(reverse(user_id), 8)) AS server
   FROM mid_info_all
   WHERE ds = '{date}' ) t1 LEFT semi
JOIN
  ( SELECT a.user_id
   FROM
     ( SELECT user_id
      FROM raw_activeuser
      WHERE ds='{date}' ) a
   LEFT OUTER JOIN
     ( SELECT DISTINCT user_id
      FROM raw_activeuser
      WHERE ds >= '{date_in_1days}'
        AND ds <= '{date_in_7days}' ) b ON a.user_id= b.user_id
   WHERE b.user_id IS NULL ) t2 ON t1.user_id = t2.user_id
GROUP BY server
'''.format(**{
        'date': date,
        'date_in_1days': ds_add(date, 1),
        'date_in_7days': ds_add(date, 7),
    })

    print act_loss_sql
    act_loss_df = hql_to_df(act_loss_sql)
    act_loss_df['ds'] = date
    columns = ['ds', 'server', 'act_sum', 'vip0', 'vip1', 'vip2', 'vip3',
               'vip4', 'vip5', 'vip6', 'vip7', 'vip8', 'vip9', 'vip10',
               'vip11', 'vip12', 'vip13', 'vip14', 'vip15']
    act_loss_df = act_loss_df[columns]
    print act_loss_df
    # 更新MySQL表
    del_sql = 'delete from {0} where ds="{1}"'.format(table, date)
    update_mysql(table, act_loss_df, del_sql)

    return act_loss_df


if __name__ == '__main__':
    settings_dev.set_env('metal_test')
    date = '20160523'
    result = dis_act_loss_info(date)
