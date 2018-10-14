#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
@Time    : 16-12-8 下午5:33
@Author  : Andy 
@File    : channel_conversion_rate_finally_proving_b.py
@Software: PyCharm
Description :
'''

from utils import hql_to_df, ds_add, update_mysql, hql_to_df,date_range
import settings_dev
import pandas as pd

def get_data(start_date,end_date):
    demo_sql = '''
        select account,device_mark as device,regexp_replace(substr(reg_time,1,11),'-','')as reg_ds
        from parse_info
        where ds>='{start_date}'
        and ds<='{end_date}'
        and regexp_replace(substr(reg_time,1,11),'-','') >='20161110'
        and (device_mark = "02:00:00:00:00:00" or device_mark = '00:00:00:00:00:00' or device_mark = '')
        group by device_mark,reg_ds,account
    '''.format(start_date = start_date,end_date = end_date)
    print demo_sql
    demo_df = hql_to_df(demo_sql)


    pd.DataFrame(demo_df).to_excel('/home/kaiqigu/桌面/hive中的设备数据(模拟器设备).xlsx',index=False)

# date_list = date_range(start_date,end_date)
if __name__ == '__main__':
    settings_dev.set_env('dancer_pub')
    get_data("20161109","20161207")
    print "end"

