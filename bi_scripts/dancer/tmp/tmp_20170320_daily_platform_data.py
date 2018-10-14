#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Hu Chunlong
Description : 武娘 日常数据，韩鹏专用脚本。
create_date : 2016.07.17
'''
from sqlalchemy.engine import create_engine
import pandas as pd

def hql_to_df(sql):
    impala_url = 'impala://192.168.1.47:21050/dancer_pub'
    engine = create_engine(impala_url)
    connection = engine.raw_connection()
    print '''===RUNNING==='''
    df = pd.read_sql(sql, connection)
    # print df
    return df
    connection.close()

def dis_daily_data(start_date, date):

    info_sql = '''
        select t1.user_id, t1.ds, t2.platform from (
        select user_id, ds from parse_info where ds>='{start_date}' and ds<='{date}') t1 left join
        (select user_id, platform from parse_actionlog where ds>='{start_date}' and ds<='{date}' group by user_id, platform) t2
        on t1.user_id = t2.user_id
    '''.format(date=date, start_date=start_date)
    info_df = hql_to_df(info_sql)
    print info_df.head(10)

    # reg_df = info_df[info_df['regtime'] == info_df['ds']]
    # reg_df['reg_num'] = reg_df['user_id']
    # print reg_df.head(10)
    reg_sql = '''
        select user_id, user_id as reg_num, regexp_replace(to_date(reg_time), '-', '') as ds from parse_info where ds>='{start_date}' and ds<='{date}'
        and regexp_replace(to_date(reg_time), '-', '')>='{start_date}' and regexp_replace(to_date(reg_time), '-', '')<='{date}'
    '''.format(date=date, start_date=start_date)
    reg_df = hql_to_df(reg_sql)
    print reg_df.head(10)

    #收入
    pay_sql = '''
        select user_id, user_id as pay_num, sum(order_money) as pay, ds from raw_paylog where ds>='{start_date}' and ds<='{date}' and platform_2 != 'admin_test' and order_id not like '%testktwwn%' group by user_id, ds
    '''.format(date=date, start_date=start_date)
    pay_df = hql_to_df(pay_sql)
    print pay_df.head(10)

    # 新增充值人数
    new_sql = '''
        select user_id, user_id as new_pay, ds from raw_paylog where ds>='{start_date}' and ds<='{date}' and user_id not in (select distinct user_id from raw_paylog where ds<'{start_date}') group by user_id, ds
    '''.format(date=date, start_date=start_date)
    new_df = hql_to_df(new_sql)
    print new_df.head(10)

    data = info_df.merge(reg_df, on=['ds', 'user_id'], how='left').merge(pay_df, on=['ds', 'user_id'], how='left').merge(new_df, on=['ds', 'user_id'], how='left')
    print data.head(10)

    data = data.groupby(['ds', 'platform']).agg({
        'user_id': lambda g: g.count(),
        'reg_num': lambda g: g.count(),
        'pay_num': lambda g: g.count(),
        'pay': lambda g: g.sum(),
        'new_pay': lambda g: g.count(),
    }).rename(columns={'user_id': 'dau'}).reset_index()
    data['rate'] = data['pay_num']* 1.0 / data['dau']
    data['arpu'] = data['pay'] * 1.0 / data['dau']
    data['arppu'] = data['pay'] * 1.0 / data['pay_num']

    print data.head(10)
    return data

if __name__ == '__main__':

    start_date = raw_input("start_date:")
    date = raw_input("date:")
    result = dis_daily_data(start_date, date)
    directory = raw_input('directory:')
    result.to_excel('%s.xlsx'%directory)