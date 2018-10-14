#!/usr/bin/env python  
# encoding: utf-8  

""" 
@author: Andy 
@site:  
@software: PyCharm 
@file: tmp.py 
@time: 17/11/20 下午4:39 
"""

import settings_dev
from utils import ds_add
from utils import hql_to_df
from utils import update_mysql
from sqls_for_games.superhero import gs_sql
import pandas as pd


def data_reduce():
    settings_dev.set_env('superhero_vt')
    # info_sql = '''
    # select substring(t1.ds,1,6) as mon,t1.vip,count(distinct t1.account) as num from (
    # select ds,account,max(vip_level) as vip  from raw_info where ds>='20170501' group by ds,account
    # )t1
    # group by mon,t1.vip
    # '''
    # info_sql = '''
    #     select t1.mon,t1.vip,count(distinct t1.uid) as num from (
    #     select substring(ds,1,6) as mon,uid,max(vip_level) as vip  from raw_info where ds>='20170501' group by mon,uid
    #     )t1
    #     group by t1.mon,t1.vip
    # '''
    info_sql = '''
        select t1.uid,t1.vip_level,t1.create_time,t1.fresh_time,t2.max_order_time from (
            select uid,vip_level,create_time,fresh_time from mid_info_all where ds='20171123' and fresh_time>='2017-06-01 00:00:00' group by uid,vip_level,create_time,fresh_time
        )t1 left outer join(
            select uid,max(order_time) as max_order_time from raw_paylog where ds>='20140101' and ds<='20171122' group by uid
        )t2 on t1.uid=t2.uid
        group by t1.uid,t1.vip_level,t1.create_time,t1.fresh_time,t2.max_order_time
    '''
    info_df = hql_to_df(info_sql)
    print info_df.head(3)

    pay_sql = '''
         select uid,sum(order_money)as order_money from raw_paylog where ds>='20170601'  group by uid
    '''
    pay_df = hql_to_df(pay_sql)
    print pay_df.head(3)

    coin_sql = '''
        select t1.uid from (
          select ds,uid,sum(order_coin)  as order_coin  from raw_paylog where ds>='20170601' and platform_2 != 'admin_test' group by ds,uid
        )t1 where t1.order_coin>=40000
        group by t1.uid
    '''
    coin_df = hql_to_df(coin_sql)
    print coin_df

    mon_sql = '''
        select substring(ds,1,6) as mon,uid,sum(order_money)as mon_order_money from raw_paylog where ds>='20170601'  group by mon,uid
    '''
    mon_df = hql_to_df(mon_sql)
    print mon_df.head(3)


    result_df = coin_df.merge(info_df, on=['uid', ], how='left')
    result_df = result_df.merge(pay_df, on=['uid', ], how='left')
    result_df = result_df.merge(mon_df, on=['uid', ], how='left')

    return result_df

if __name__ == '__main__':
    result = data_reduce()
    result.to_excel('/Users/kaiqigu/Documents/Superhero/超级英雄-越南-6月中间大R数据40000_20171124.xlsx')