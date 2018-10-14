#!/usr/bin/env python  
# encoding: utf-8  

""" 
@author: Andy 
@site:  
@software: PyCharm 
@file: demo_q.py 
@time: 18/3/20 上午9:54 
"""


import calendar
import settings_dev
from utils import ds_add, hql_to_df, update_mysql, hqls_to_dfs, date_range
import pandas as pd
import sys
reload(sys)
sys.setdefaultencoding("utf-8")

def data_reduce():
    info_sql = '''
      select t1.uid,t1.order_money,t1.rn,t2.account,t2.nick,t2.create_time,t2.fresh_time from (
        select uid,sum(order_money) as order_money,row_number() over(order by sum(order_money) desc ) as rn from raw_paylog where ds>='20180101' and platform_2 <> 'admin_test' and platform_2 <> 'admin' group by uid
      )t1 left outer join(
        select uid,account,nick,create_time,fresh_time from mid_info_all where ds='20180319' group by uid,account,nick,create_time,fresh_time 
      )t2 on t1.uid=t2.uid
      group by t1.uid,t1.order_money,t1.rn,t2.account,t2.nick,t2.create_time,t2.fresh_time
    '''
    print info_sql
    info_df = hql_to_df(info_sql)
    print info_df.head()
    info_df.to_excel(r'/Users/kaiqigu/Documents/Superhero/superhero-bi-2018-pay-rank_20180320-3.xlsx', index=False,)
    return info_df

if __name__ == '__main__':

    for platform in ['superhero_bi',]:
        settings_dev.set_env(platform)
        result = data_reduce()
        # result.to_csv(r'/Users/kaiqigu/Documents/Superhero/超英-越南-箱子数据_20180307.csv')
        result.to_excel(r'/Users/kaiqigu/Documents/Superhero/superhero-bi-2018-pay-rank_20180320-4.xlsx', index=False,encoding='utf-8')
    print "end"
