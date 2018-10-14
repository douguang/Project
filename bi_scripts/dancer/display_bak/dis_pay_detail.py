#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Hu Chunlong
Description : 充值详情
create_date : 2016.07.18
'''
from utils import hql_to_df, ds_add, update_mysql
import settings_dev
import pandas as pd
from pandas import DataFrame

def dis_pay_detail(date):
    sql = '''
    SELECT '{date}' as ds,
           scheme_id,
           vip_level,
           sum(pay_user_num) as pay_user_num,
           sum(pay_times) as pay_times
    FROM
      (SELECT scheme_id,
              user_id,
              count(DISTINCT user_id) AS pay_user_num,
              count(user_id) AS pay_times
       FROM raw_paylog
       WHERE ds = '{date}' and platform_2<>'admin_test' AND order_id not like '%testktwwn%'
       GROUP BY scheme_id,user_id ) t1
    LEFT OUTER JOIN
      (SELECT user_id,
              vip AS vip_level
       FROM parse_info
       WHERE ds = '{date}' ) t2 ON t1.user_id = t2.user_id
    GROUP BY scheme_id,vip_level
    '''.format(date=date)
    df = hql_to_df(sql)
    df = df.fillna(0)
    # 更新消费详情的MySQL表
    table = 'dis_pay_detail'
    print date,table
    del_sql = 'delete from {0} where ds="{1}"'.format(table, date)
    update_mysql(table, df, del_sql)
if __name__ == '__main__':
    settings_dev.set_env('dancer_tw')
    dis_pay_detail('20160808')
