#!/usr/bin/env python  
# encoding: utf-8  

""" 
@author: Andy 
@site:  补mid的数据
@software: PyCharm 
@file: mid_reload.py
@time: 18/3/28 下午3:02 
"""
from superhero.luigi_test import mid_hive_table
from superhero.luigi_test import mid_hive_table
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

def data_reduce(date_str,platform,task_name,server='impala'):
    settings_dev.set_env(platform)
    yestoday = ds_add(date_str, -1)
    hql =''
    if platform in ['superhero_qiku','superhero_tw']:
        hql = mid_hive_table.mid_dic[task_name].format(date=date_str,yestoday=yestoday)
    elif platform == 'superhero_bi':
        hql = mid_hive_table.bi_mid_dic[task_name].format(date=date_str, yestoday=yestoday)
    elif platform == 'superhero_mul':
        hql = mid_hive_table.mul_mid_dic[task_name].format(date=date_str, yestoday=yestoday)
    elif platform == 'superhero_vt':
        hql = mid_hive_table.vt_mid_dic[task_name].format(date=date_str, yestoday=yestoday)
    elif platform == 'superhero_self_en':
        hql = mid_hive_table.self_mid_dic[task_name].format(date=date_str, yestoday=yestoday)
    else:
        hql = mid_hive_table.foreign_mid_dic[task_name].format(date=date_str, yestoday=yestoday)

    url = ''
    if server == 'impala':
        url = impala_template.format(db=platform)
    elif server == 'hive':
        url = hive_template.format(db=platform)
    else:
        raise Exception('argument server have to be hive or impala!')

    engine = sqlalchemy.create_engine(url)
    conn = engine.raw_connection()
    try:
        if server == 'impala':
            cur = conn.cursor()
            cur.execute('INVALIDATE METADATA')
            cur.execute(hql)
            print 'sql executed!!!'
    finally:
        conn.close()

if __name__ == '__main__':
    date_str = '20180321'
    platform = 'superhero_mul'
    # mid_info_all   mid_paylog_all  mid_new_account
    task_name = 'mid_info_all'  #
    data_reduce(date_str,platform,task_name)
    print 'end '
