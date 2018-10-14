#!/usr/bin/env python
# -*- coding: UTF-8 -*-
"""
Author      : Hu Chunlong
Description : 设备转化率
create_date : 2016.12.29
"""

import settings_dev
from utils import update_mysql, hql_to_df, ds_add, date_range
import pandas as pd
from sqlalchemy.engine import create_engine


def dis_nginx_device_exchange(date, platform):
    settings_dev.set_env(platform)
    # 第一步get_user_server_list,loading no account
    step_1 = '''
    select {date} as ds, 'step_1' as step, platform_channel as pt_chl, device_mark
    from parse_nginx
    where ds = '{date}'
    and method = 'get_user_server_list'
    and account = ''
    and platform_channel != ''
    and device_mark not in (
      select device as device_mark
      from mid_info_all
      where ds = '{date_1}'
      )
    '''.format(**{
        'date': date,
        'date_1': ds_add(date, -1),
    })
    step_2 = '''
    select {date} as ds, 'step_2' as step, platform_channel as pt_chl, device_mark
    from parse_nginx
    where ds = '{date}'
    and method = 'loading'
    and account = ''
    and platform_channel != ''
    and device_mark not in (
      select device as device_mark
      from mid_info_all
      where ds = '{date_1}'
      )
    '''.format(**{
        'date': date,
        'date_1': ds_add(date, -1),
    })
    step_3 = '''
    select {date} as ds, 'step_3' as step, platform_channel as pt_chl, device_mark
    from parse_nginx
    where ds = '{date}'
    and method = 'get_user_server_list'
    and account != ''
    and platform_channel != ''
    and device_mark not in (
      select device as device_mark
      from mid_info_all
      where ds = '{date_1}'
    )
    '''.format(**{
        'date': date,
        'date_1': ds_add(date, -1),
    })
    df_1 = hql_to_df(step_1)
    df_2 = hql_to_df(step_2)
    df_3 = hql_to_df(step_3)
    # 将表合并
    use_df = pd.concat([df_1, df_2, df_3])

    result_df = use_df.groupby(['ds', 'step', 'pt_chl']).agg(
        {'device_mark': lambda g: g.nunique()}).reset_index()
    finally_df = result_df.pivot_table('device_mark', ['ds','pt_chl'], 'step').reset_index().fillna(0)
    print finally_df
    table = 'dis_nginx_device_exchange_back'
    del_sql = "DROP TABLES {0}".format(table)
    update_mysql(table, finally_df, del_sql)

    url = "mysql+pymysql://root:60aa954499f7ab@192.168.1.27/{0}".format(platform)
    engine = create_engine(url)
    connection = engine.raw_connection()
    insert_sql = "insert into dis_nginx_device_exchange SELECT * FROM dis_nginx_device_exchange_back"
    cur = connection.cursor()
    cur.execute(insert_sql)
    connection.commit()
    print 'insert'
    connection.close()
    # table = 'dis_nginx_device_exchange'
    # del_sql = "DELETE FROM '{0}'".format(table)
    # update_mysql(table, finally_df, del_sql)

if __name__ == '__main__':
    date = '20170103'
    # for date in date_range('20161221', '20161229'):
    for platform in ['sanguo_ks', 'sanguo_tw']:
        dis_nginx_device_exchange(date, platform)
