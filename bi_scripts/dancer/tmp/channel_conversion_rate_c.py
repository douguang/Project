#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
@Time    : 16-12-8 下午1:39
@Author  : Andy 
@File    : channel_conversion_rate_c.py
@Software: PyCharm
Description :  从hive取出所有的设备号和account及最早时间
'''
from utils import hql_to_df, ds_add, update_mysql, hql_to_df,date_range
import settings_dev
import pandas as pd

def get_data(start_date,end_date):
    reg_sql = '''
        select account,device_mark as device,regexp_replace(substr(reg_time,1,11),'-','')as ds
        from parse_info
        where ds>='{start_date}'
        and regexp_replace(substr(reg_time,1,11),'-','') >='{start_date}'
        and ds<='{end_date}'
        and device_mark != "02:00:00:00:00:00"
        and device_mark != '00:00:00:00:00:00'
        and device_mark != ''
        group by device_mark,ds,account
    '''.format(start_date = start_date,end_date = end_date)
    print reg_sql
    reg_df = hql_to_df(reg_sql)


    reg_df['account']=1
    pd.DataFrame(reg_df).to_excel('/home/kaiqigu/桌面/hive中的设备数据(20-06).xlsx',index=False)

# date_list = date_range(start_date,end_date)
if __name__ == '__main__':
    settings_dev.set_env('dancer_pub')
    get_data("20161109","20161207")
    print "end"
#result_df.to_excel('/Users/kaiqigu/Downloads/Excel/d3_ltv.xlsx')