#!/usr/bin/env python  
# encoding: utf-8  

""" 
@author: Andy 
@site:  付费用户留存
@software: PyCharm 
@file: pay_user_liucun.py 
@time: 18/3/29 下午4:20 
"""

import calendar
import settings_dev
from utils import ds_add, hql_to_df, update_mysql, hqls_to_dfs, date_range
import pandas as pd

def data_reduce():

    reg_sql = '''
    select t1.reg_ds,t1.dnu,t2.pay_num from (
      select to_date(reg_time) as reg_ds,count(distinct user_id) as dnu from mid_info_all where ds='20180328' group by reg_ds
    )t1 left outer join(
      select to_date(reg_time) as reg_ds,count(distinct user_id) as pay_num from mid_info_all where ds='20180328' and user_id in (select user_id from raw_paylog where ds>='20180314' and platform_2 <> 'admin' and platform_2 <> 'admin_test') group by reg_ds
    )t2 on t1.reg_ds=t2.reg_ds
    group by t1.reg_ds,t1.dnu,t2.pay_num
    '''
    print reg_sql
    reg_df = hql_to_df(reg_sql)
    print reg_df.head()

    info_sql = '''
        select ds,to_date(reg_time) as reg_ds,count(distinct user_id) as pay_user_num from raw_info where ds>='20180314' and user_id in (select user_id from raw_paylog where ds>='20180314' and platform_2 <> 'admin' and platform_2 <> 'admin_test') group by ds,reg_ds 
    '''
    print info_sql

    info_df = hql_to_df(info_sql)
    print info_df.head()



    result = reg_df.merge(info_df, on=['reg_ds',], how='left')

    result.to_excel(r'/Users/kaiqigu/Documents/Sanguo/合金装甲-国内版-付费玩家留存数据_20180329.xlsx', index=False)


if __name__ == '__main__':
    for platform in ['metal_pub',]:
        settings_dev.set_env(platform)
        data_reduce()
    print "end"
