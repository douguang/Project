#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Hu Chunlong
Description : 1.国服武娘的用户；2.渠道：ioskvgames；3.近七日活跃的；4.vip等级>=7的； 客服自用查询脚本
create_date : 2017.05.22
'''
from sqlalchemy.engine import create_engine
import pandas as pd
import datetime

def ds_add(date, delta, date_format='%Y%m%d'):
    return datetime.datetime.strftime(
        datetime.datetime.strptime(date, date_format) +
        datetime.timedelta(delta), date_format)

def hql_to_df(sql, platform):
    impala_url = 'impala://192.168.1.47:21050/%s'%platform
    engine = create_engine(impala_url)
    connection = engine.raw_connection()
    print '''===RUNNING==='''
    df = pd.read_sql(sql, connection)
    # print df
    return df
    connection.close()

def dis_daily_data(platform, date):

    info_sql = '''
        select user_id, account, vip, level from mid_info_all where ds='{date}' and account like 'ioskvgames%' and regexp_replace(to_date(act_time), '-', '')>='{date_7}' and vip>=7
    '''.format(date=date, date_7=ds_add(date, -6))
    info_df = hql_to_df(info_sql, platform)
    print info_sql
    # print info_df.head(10)
    return info_df

if __name__ == '__main__':

    platform = raw_input("platform:")
    date = raw_input("date:")
    result = dis_daily_data(platform, date)
    directory = raw_input('file:')
    result.to_excel(r'C:\Users\Administrator\Desktop\%s.xlsx'%directory)