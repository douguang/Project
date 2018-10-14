#!/usr/bin/env python  
# encoding: utf-8  

""" 
@author: Andy 
@site:  分包留存数据
@software: PyCharm 
@file: appid_liucun_data.py 
@time: 18/1/17 下午5:16 
"""

import calendar
import settings_dev
from utils import ds_add, hql_to_df, update_mysql, hqls_to_dfs, date_range
import datetime
import pandas as pd


def data_reduce():

    info_sql = '''
    select t1.*,t2.ds from (
      select regexp_replace(to_date(reg_time),'-','') as reg_ds,account,user_id,appid from mid_info_all where ds='20180116' and appid = 'cnwnioshd' group by reg_ds,account,user_id,appid 
    )t1 left outer join(
      select user_id,ds from parse_info where ds>='20160101' group by user_id,ds
    )t2 on t1.user_id=t2.user_id
    '''
    print info_sql
    info_df = hql_to_df(info_sql)
    print info_df.head()
    result_df = info_df.groupby(['appid','reg_ds','ds',]).agg({
        'user_id': lambda g: g.nunique(),
    }).reset_index().rename(columns={'user_id': 'user_id_num',})

    return result_df

if __name__ == '__main__':
    #dis_kaiqigu_pay_detail('20170102')
    for platform in ['dancer_pub',]:
        settings_dev.set_env(platform)
        result = data_reduce()
        result.to_excel(r'/Users/kaiqigu/Documents/Dancer/武娘-国内-cnwnioshd包留存_20180117.xlsx', index=False)
    print "end"