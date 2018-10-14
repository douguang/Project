#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Hu Chunlong(暂时搁置)
Description : 充值详情
create_date : 2016.07.18
'''
from utils import hqls_to_dfs, ds_add, update_mysql
import settings_dev
import pandas as pd
from pandas import DataFrame

def dis_vip_level_detail(date):
    old_vip_sql = '''
    SELECT user_id,
           vip AS vip_level,
           '{yestoday}' AS ds
    FROM mid_info_all
    WHERE ds = '{yestoday}'
      AND vip > 0
    '''.format(yestoday=ds_add(date, -1))
    new_vip_sql = '''
    SELECT user_id,
           vip AS vip_level,
           '{date}' AS ds
    FROM mid_info_all
    WHERE ds = '{date}'
      AND vip > 0
      AND user_id NOT IN
      (SELECT user_id
       FROM mid_info_all
       WHERE ds = '{yestoday}'
         AND vip > 0)
    '''.format(date=date, yestoday=ds_add(date, -1))
    pay_sql = '''
    SELECT t1.user_id,
           pay_rmb,
           vip AS vip_level
    FROM
      (SELECT user_id,
              sum(order_money) AS pay_rmb
       FROM raw_paylog
       WHERE ds = '{date}'
       and platform_2<>'admin_test' AND order_id not like '%testktwwn%'
       GROUP BY user_id) t1
    LEFT OUTER JOIN
      (SELECT user_id,
              vip
       FROM mid_info_all
       WHERE ds = '{date}') t2 ON t1.user_id = t2.user_id
    '''.format(date=date)
    old_vip_df, new_vip_df, pay_df = hqls_to_dfs([old_vip_sql, new_vip_sql, pay_sql])
    print pay_df
    mid_df = pay_df.groupby('vip_level').sum().reset_index()
    mid_df = mid_df.T
    # mid_df['income'] = pay_df['pay_rmb'].sum()
    # mid_df['new_vip_num'] = new_vip_df.user_id.count()
    # mid_df['vip_num'] = new_vip_df.user_id.count() + old_vip_df.user_id.count()
    print mid_df
    # 更新消费详情的MySQL表
    # table = 'dis_vip_level_detail'
    # del_sql = 'delete from {0} where ds="{1}"'.format(table, date)
    # update_mysql(table, df, del_sql)

if __name__ == '__main__':
    settings_dev.set_env('dancer_tw')
    dis_vip_level_detail('20160802')
