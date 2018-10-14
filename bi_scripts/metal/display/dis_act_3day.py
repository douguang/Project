#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Hu Wenchao
Description : 3日活跃人数汇总表
Name        : dis_d3_act_user_num
'''
import settings_dev
from utils import hql_to_df, update_mysql, ds_add, date_range

def dis_act_3day(date):
    # table = 'dis_act_3day'
    table = 'dis_d3_act_user_num'
    act_3day_sql = '''
    SELECT '{date}' AS ds,
           vip,
           reverse(substring(reverse(user_id), 8)) AS server,
           count(user_id) AS d3_act_user_num
    FROM
      ( SELECT user_id,
               vip
       FROM mid_info_all
       WHERE ds = '{date}') t1 LEFT semi
    JOIN
      ( SELECT DISTINCT user_id
       FROM raw_activeuser
       WHERE ds >= '{date_in_3days}'
         AND ds <= '{date}') t2 ON t1.user_id = t2.user_id
    GROUP BY vip,
             server'''.format(**{
        'date': date,
        'date_in_3days': ds_add(date, -2),
    })
    print act_3day_sql
    act_3day_df = hql_to_df(act_3day_sql)
    #print act_3day_df

    # 更新MySQL表
    del_sql = 'delete from {0} where ds="{1}"'.format(table, date)
    update_mysql(table, act_3day_df, del_sql)

if __name__ == '__main__':
    for platform in ['sanguo_ks', 'sanguo_tx', 'sanguo_ios', 'sanguo_tw']:
        settings_dev.set_env(platform)
        for date in date_range('20160720', '20160731'):
            dis_act_3day(date)
