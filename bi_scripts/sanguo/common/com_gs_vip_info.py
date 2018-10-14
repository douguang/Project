#!/usr/bin/env python
# -*- coding: UTF-8 -*-

import pandas as pd
from utils import hql_to_df, update_mysql, ds_add, date_range
import settings
import datetime
import time

def sanguo_vip_info(date):
    sql = '''
    SELECT t1.user_id,
           level,
           vip,
           last_login,
           pay_all,
           last_pay
    FROM
      ( SELECT user_id,
               LEVEL,
               vip,
               to_date(act_time) AS last_login
       FROM mid_info_all
       WHERE ds = '{date}'
         AND vip >= 6
         AND level > 10
         AND user_id NOT IN
           ( SELECT user_id
            FROM raw_paylog
            WHERE platform_2 = 'admin_test' ) ) t1
    LEFT OUTER JOIN
      ( SELECT user_id,
               sum(order_money) AS pay_all,
               max(to_date(order_time)) AS last_pay
       FROM raw_paylog
       WHERE platform_2 != 'admin_test'
       GROUP BY user_id ) t2 ON t1.user_id = t2.user_id
    '''.format(date=date)
    bi_df = hql_to_df(sql)
    gs_df = pd.read_excel(r'E:\My_Data_Library\sanguo_vip_info.xls')
    result_df = bi_df.merge(gs_df,on='user_id',how='left')
    print result_df
    result_df.to_excel(r'E:\My_Data_Library\sanguo_vip_info_20160920.xlsx')
if __name__ == '__main__':
    settings.set_env('sanguo_ks')
    sanguo_vip_info('20160920')
