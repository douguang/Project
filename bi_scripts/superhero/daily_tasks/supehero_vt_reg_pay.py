#!/usr/bin/env python  
# encoding: utf-8  

""" 
@author: Andy 
@site:  
@software: PyCharm 
@file: supehero_vt_reg_pay.py 
@time: 17/12/29 下午1:17 
"""


import settings_dev
from utils import ds_add
from utils import hql_to_df
from utils import update_mysql
from sqls_for_games.superhero import gs_sql
import pandas as pd


def data_reduce():
    # info_sql = '''
    # select t1.reg_ds,t1.account,t1.uid,t1.rank,t2.order_money,case when t1.rank=1  then 'first' else 'more' end as is_not_gun,reverse(substring(reverse(t1.uid),8)) as server from (
    #   select to_date(create_time) as reg_ds,account,uid,row_number() over(partition by to_date(create_time),account order by account) as rank from raw_info where ds>='20171221' and to_date(create_time)>='2017-12-21' group by reg_ds,account,uid
    # )t1 left outer join(
    #   select to_date(order_time) as pay_ds,uid,sum(order_money) as order_money from raw_paylog where ds>='20171221' and platform_2 != 'admin_test' group by pay_ds,uid
    # )t2 on (t1.reg_ds=t2.pay_ds and t1.uid=t2.uid)
    # group by t1.reg_ds,t1.account,t1.uid,t1.rank,t2.order_money,is_not_gun,server
    # '''

    info_sql = '''
    select t1.reg_ds,t1.account,t1.uid,t1.rank,t2.order_money,case when t1.rank=1  then 'first' else 'more' end as is_not_gun,reverse(substring(reverse(t1.uid),8)) as server from (
             select to_date(create_time) as reg_ds,account,uid,row_number() over(partition by account order by to_date(create_time)) as rank from mid_info_all 
             where ds='20171228'  group by reg_ds,account,uid 
    )t1 left outer join(
              select to_date(order_time) as pay_ds,uid,sum(order_money) as order_money from raw_paylog where ds>='20171221' and platform_2 != 'admin_test' group by pay_ds,uid
    )t2 on (t1.reg_ds=t2.pay_ds and t1.uid=t2.uid)
    where t1.reg_ds >='2017-12-21' 
    group by t1.reg_ds,t1.account,t1.uid,t1.rank,t2.order_money,is_not_gun,server
    '''

    info_df = hql_to_df(info_sql)
    print info_df.head(3)

    res_df = info_df.groupby(['reg_ds', 'is_not_gun',]).agg(
        {'uid': lambda g: g.nunique(),'order_money': lambda g: g.sum()}).reset_index()
    reg_num_df = info_df.groupby(['reg_ds',]).agg(
        {'uid': lambda g: g.nunique(),}).rename(columns={'uid': 'reg_num'}).reset_index()

    result_df =  info_df.dropna()
    result_num_df = result_df.groupby(['reg_ds',]).agg(
        {'uid': lambda g: g.nunique(), }).rename(columns={'uid': 'payer_nunique_num'}).reset_index()
    result_df = result_df.groupby(['reg_ds', 'is_not_gun', ]).agg(
        {'uid': lambda g: g.nunique(),}).rename(columns={'uid': 'payer_num'}).reset_index()



    result = res_df.merge(result_df, on=['reg_ds','is_not_gun'])
    result = result.merge(reg_num_df, on=['reg_ds',])
    result = result.merge(result_num_df, on=['reg_ds',])

    print result.head(3)

    result.to_excel('/Users/kaiqigu/Documents/Superhero/超级英雄-越南-新增用户、充值及滚服数据uid_20171229.xlsx')


if __name__ == '__main__':
    settings_dev.set_env('superhero_vt')
    data_reduce()