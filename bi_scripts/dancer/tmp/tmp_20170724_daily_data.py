#!/usr/bin/env python
# -- coding: UTF-8 --
'''
@Time : 2017/7/24 0024 12:22
@Author : Zhang Yongchen
@File : 韩鹏脚本，武娘多语言，输入起止时间以及时间间隔，如：
dis_daily_data(20170710, 20170711, 10)
则取20170710-20170711之间的日常数据，且把开服1-10日算新服，大于10日的算老服
@Software: PyCharm Community Edition
Description :
'''

from sqlalchemy.engine import create_engine
import pandas as pd
import datetime

def hql_to_df(sql):
    impala_url = 'impala://192.168.1.47:21050/dancer_mul'
    engine = create_engine(impala_url)
    connection = engine.raw_connection()
    print '''===RUNNING==='''
    print sql
    df = pd.read_sql(sql, connection)
    # print df
    return df
    connection.close()

def dis_daily_data(start_date, date, day_limit):

    day_limit = int(day_limit)

    # 渠道、服务器
    info_sql = '''
        select user_id,
        language_sort,
        reverse(substr(reverse(user_id), 8)) as server, ds from parse_info where ds>='{start_date}' and ds<='{date}'
    '''.format(date=date, start_date=start_date)
    info_df = hql_to_df(info_sql)
    print info_df.head(10)
    # info_df.to_excel(r'E:\Data\output\dancer\info.xlsx')
    # 注册
    reg_sql = '''
        select user_id, user_id as reg_num, regexp_replace(to_date(reg_time), '-', '') as ds from parse_info where ds>='{start_date}' and ds<='{date}'
        and regexp_replace(to_date(reg_time), '-', '')>='{start_date}' and regexp_replace(to_date(reg_time), '-', '')<='{date}' group by user_id,  regexp_replace(to_date(reg_time), '-', '')
    '''.format(date=date, start_date=start_date)
    reg_df = hql_to_df(reg_sql)
    print reg_df.head(10)

    #收入
    pay_sql = '''
        select user_id, user_id as pay_num, sum(order_money) as pay, ds from raw_paylog where ds>='{start_date}' and ds<='{date}' and platform_2<>'admin_test' group by user_id, ds
    '''.format(date=date, start_date=start_date)
    pay_df = hql_to_df(pay_sql)
    print pay_df.head(10)
    # pay_df.to_excel(r'E:\Data\output\dancer\pay.xlsx')

    # 新增充值人数
    new_sql = '''
        select user_id, user_id as new_pay, ds from raw_paylog where ds>='{start_date}' and ds<='{date}' and user_id not in (select distinct user_id from raw_paylog where ds<'{start_date}') group by user_id, ds
    '''.format(date=date, start_date=start_date)
    new_df = hql_to_df(new_sql)
    print new_df.head(10)

    # 开服时间
    server_sql = '''
        select reverse(substr(reverse(user_id), 8)) as server, min(ds) as server_date from parse_info where ds<='{date}' group by server
    '''.format(date=date)
    server_df = hql_to_df(server_sql)
    print server_df.head(10)

    data = info_df.merge(reg_df, on=['ds', 'user_id'], how='left').merge(pay_df, on=['ds', 'user_id'], how='left').merge(new_df, on=['ds', 'user_id'], how='left')
    print data.head(10)
    data = data.merge(server_df, on='server', how='left')
    print data.head(10)
    data['day'] = (pd.to_datetime(data['ds']) - pd.to_datetime(data['server_date'])).dt.days + 1
    print data
    new_df = data[data['day'] <= day_limit]
    new_df['server'] = '新服'
    old_df = data[data['day'] > day_limit]
    old_df['server'] = '老服'
    data = pd.concat([new_df, old_df])
    data = data.groupby(['ds', 'language_sort', 'server']).agg({
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

    # dis_daily_data('20170701', '20170702', 10)

    start_date = raw_input("start_date:")
    date = raw_input("date:")
    day_limit = raw_input("day_limit:")
    result = dis_daily_data(start_date, date, day_limit)
    f_name = raw_input("name is ?")
    result.to_excel('%s.xlsx' % f_name,index=False)
