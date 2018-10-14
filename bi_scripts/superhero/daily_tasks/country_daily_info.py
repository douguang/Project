#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
@Time    : 2018/2/8 0008 23:04
@Author  : Andy 
@File    : country_daily_info.py
@Software: PyCharm
Description :
'''


import calendar
import settings_dev
from utils import ds_add, hql_to_df, update_mysql, hqls_to_dfs, date_range


def data_reduce():

    act_sql = '''
        select ds,uid from raw_action_log where ds>='20180206' group by ds,uid
    '''
    print act_sql
    act_df = hql_to_df(act_sql)
    print act_df.head()

    info_sql = '''
        select uid,regexp_replace(to_date(create_time),'-','') as reg_ds,ip from raw_info where ds>='20180206' group by uid,reg_ds,ip
    '''
    print info_sql
    info_df = hql_to_df(info_sql)
    print info_df.head()
    act_df = act_df.merge(info_df, on=['uid', ], how='left')



    dau_sql = '''
    select ds,count(distinct uid) as dau from raw_info where ds>='20180206' group by ds
    '''
    print dau_sql
    dau_df = hql_to_df(dau_sql)
    print dau_df.head()

    dnu_sql = '''
        select regexp_replace(to_date(create_time),'-','') as ds,count(distinct account) as dnu from mid_info_all where ds='20180207' group by ds
    '''
    print dnu_sql
    dnu_df = hql_to_df(dnu_sql)
    print dnu_df.head()

    pay_sql = '''
        select ds,count(distinct pay_pt) as pau,sum(order_money) as order_money from raw_paylog where ds>='20180206' and product_id <> 'admin_test' group by ds
    '''
    print pay_sql
    pay_df = hql_to_df(pay_sql)
    print pay_df.head()

    reg_pay_sql = '''
        select t1.ds,t1.uid,t1.order_money,t2.reg_ds from (
        select ds,pay_pt as uid,sum(order_money) as order_money from raw_paylog where ds>='20180206' and product_id <> 'admin_test' group by ds,uid
        )t1 left outer join(
        select uid,regexp_replace(to_date(create_time),'-','') as reg_ds from mid_info_all where ds='20180207' group by uid,reg_ds
        )t2 on t1.uid=t2.uid
        group by t1.ds,t1.uid,t1.order_money,t2.reg_ds
    '''
    print reg_pay_sql
    reg_pay_df = hql_to_df(reg_pay_sql)
    print reg_pay_df.head()
    reg_pay_df = reg_pay_df[reg_pay_df.ds==reg_pay_df.reg_ds]
    print reg_pay_df.head()
    reg_pay_df = reg_pay_df.groupby(['ds',]).agg(
        {'uid': lambda g: g.nunique(),
         'order_money': lambda g: g.sum()}).reset_index().rename(columns={'uid': 'reg_uid_num','order_money': 'reg_order_money ',})
    print reg_pay_df.head()



    result = dau_df.merge(dnu_df, on=['ds',], how='left')
    result = result.merge(pay_df, on=['ds',], how='left')
    result = result.merge(reg_pay_df, on=['ds',], how='left')

    return result

if __name__ == '__main__':
    #dis_kaiqigu_pay_detail('20170102')
    for platform in ['superhero_mul',]:
        settings_dev.set_env(platform)
        result = data_reduce()
        result.to_excel(r'/Users/kaiqigu/Documents/Superhero/超级英雄-英文版-次日数据-2_20180208.xlsx', index=False)
    print "end"

