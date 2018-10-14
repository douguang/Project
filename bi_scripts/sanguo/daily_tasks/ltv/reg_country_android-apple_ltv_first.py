#!/usr/bin/env python  
# encoding: utf-8  

""" 
@author: Andy 
@site:  
@software: PyCharm 
@file: reg_country_android-apple_ltv_first.py 
@time: 17/11/14 下午5:03 
"""
from utils import date_range, ds_add, hql_to_df
import settings_dev
import pandas as pd
from ipip import *
import sqlalchemy


# 数据库连接 url 模板
hive_template = 'hive://192.168.1.8:10000/{db}'
impala_template = 'impala://192.168.1.47:21050/{db}'
hdfs_url = 'http://192.168.1.8:50070'
# 导入hive文件的位置
hive_path = '/user/hive/warehouse/{db}.db/{table}/ds={date_str}/{filename}'

def data_reduce(first_date,end_date,server='impala'):
    settings_dev.set_env('sanguo_tl')
    info_sql = '''
        INSERT overwrite TABLE user_identifier_info partition (ds='{end_date}')
        SELECT user_id,
               identifier,ip
        FROM
          ( SELECT *,
                   row_number() over(partition BY user_id
                                     ORDER BY user_id DESC) AS rn
           FROM
             ( SELECT user_token as user_id, identifier,ip
              FROM parse_nginx
              WHERE ds >= '{first_date}' and  ds <= '{end_date}'  and user_token != '' and identifier != '' group by user_id, identifier,ip
              UNION ALL SELECT user_id, identifier,ip
              FROM user_identifier_info
              WHERE ds = '{first_date}' 
             ) t1
          ) t2
        WHERE rn = 1
    '''.format(first_date=first_date,end_date=end_date)
    print info_sql
    db = settings_dev.platform
    url = ''
    if server == 'impala':
        url = impala_template.format(db=db)
    elif server == 'hive':
        url = hive_template.format(db=db)
    else:
        raise Exception('argument server have to be hive or impala!')

    engine = sqlalchemy.create_engine(url)
    conn = engine.raw_connection()
    try:
        if server == 'impala':
            cur = conn.cursor()
            cur.execute('INVALIDATE METADATA')
            cur.execute(info_sql)
            print 'sql executed!!!'
    finally:
        conn.close()


if __name__ == '__main__':
    # first_date为hive库中最新的user_identifier_info的记录
    first_date = '20180227'
    # end_date为最新的数据日期
    end_date = '20180329'
    data_reduce(first_date,end_date)
    print 'end '