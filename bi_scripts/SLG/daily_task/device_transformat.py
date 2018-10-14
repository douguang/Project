#!/usr/bin/env python  
# encoding: utf-8  

""" 
@author: Andy 
@site:  设备转换率
@software: PyCharm 
@file: device_transformat.py 
@time: 18/1/20 下午4:12 
"""

from utils import hqls_to_dfs, get_rank, hql_to_df, date_range
import pandas as pd
from utils import ds_delta
import settings_dev
from pandas import DataFrame
import time
import datetime

import sys
reload(sys)
sys.setdefaultencoding('utf8')

def data_reduce():
    info_sql = '''
        select t1.uid,t1.a_typ,t1.lt,t1.rn,t2.reg_ds,t2.app_id from (
        select a_usr as uid,a_typ,lt,row_number() over(partition by a_usr order by lt desc) as rn  from parse_actionlog where ds='20180119'
        )t1 left outer join(
          select uid,to_date(reg_time)as reg_ds,app_id from mid_info_all where ds='20180119'  group by uid,reg_ds,app_id
          )t2 on t1.uid=t2.uid
        where t1.rn=1 and t1.uid not like '#%' 
    '''
    print info_sql
    info_df = hql_to_df(info_sql)
    print info_df.head()
    info_df.to_excel('/Users/kaiqigu/Documents/slg/slg-mul-info_20180120.xlsx')

    # info_df = info_df[info_df.reg_ds == '2018-01-19']
    result_df = info_df.groupby(['app_id','a_typ']).agg({
        'uid': lambda g: g.nunique(),
    }).reset_index().rename(columns={'uid': 'uid_num',})

    return result_df



if __name__ == '__main__':
    for platform in ['slg_mul', ]:
        settings_dev.set_env(platform)
        date = '20180119'
        res = data_reduce()
        res.to_excel(r'/Users/kaiqigu/Documents/slg/星战帝国-多语言-设备停留2_20180120.xlsx', index=False)
    print "end"