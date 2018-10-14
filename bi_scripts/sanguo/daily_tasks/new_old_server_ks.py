#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
@Time    : 2017/8/24 0024 15:22
@Author  : Andy 
@File    : new_old_server_ks.py
@Software: PyCharm
Description :
'''


import calendar
import settings_dev
from utils import ds_add, hql_to_df, update_mysql, hqls_to_dfs, date_range


def data_reduce():
    # 日活
    act_info_sql = '''
        select ds,reverse(substring(reverse(user_id),8)) as server_id,count(distinct user_id) as dau from raw_info where ds>='20170809' and ds<='20170827' group by ds,server_id
    '''
    print act_info_sql
    act_info_df = hql_to_df(act_info_sql)
    print act_info_df.head()

    # 新老服 864000=10天
    is_new_old_server_sql = '''
    select t3.ds,t3.server_id,case when t3.ds_diff  < 864000 then 1 else 0 end as is_not_new from(
        select t1.ds,t1.server_id,t1.ds_unix-t2.first_ds as ds_diff
        from (
        select ds,unix_timestamp(ds,'yyyyMMdd') as ds_unix,reverse(substring(reverse(user_id),8)) as server_id from raw_info where ds>='20170103' group by ds,ds_unix,server_id
        )t1 left outer join(
        select reverse(substring(reverse(user_id),8)) as server_id,unix_timestamp(min(ds),'yyyyMMdd') as first_ds from raw_info where ds>='20170103' group by server_id
        )t2 on t1.server_id=t2.server_id
        group by  t1.ds,t1.server_id,ds_diff
    )t3
    group by t3.ds,t3.server_id,is_not_new
    '''
    print is_new_old_server_sql
    is_new_old_server_df = hql_to_df(is_new_old_server_sql)
    print is_new_old_server_df.head()
    is_new_old_server_df = is_new_old_server_df[is_new_old_server_df['ds'] >'20170808']
    print is_new_old_server_df.head()

    # 充值钻石档次
    pay_coin_distri_sql = '''
    select t1.ds,t1.server_id,t1.user_id,t1.coin_num,t2.order_money,t3.is_pay_user from (
        select ds,reverse(substring(reverse(user_id),8)) as server_id,user_id,sum(coin_num) as coin_num from raw_spendlog where ds>='20170809' group by ds,server_id,user_id
    )t1 left outer join(
        select ds,reverse(substring(reverse(user_id),8)) as server_id,user_id,sum(order_money) as order_money from raw_paylog where ds>='20170809' group by ds,server_id,user_id
    )t2 on t1.ds=t2.ds and t1.user_id=t2.user_id
    left outer join(
        select user_id,1 as is_pay_user from raw_paylog where ds>='20160419' group by user_id
    )t3 on t1.user_id=t3.user_id
    group by t1.ds,t1.server_id,t1.user_id,t1.coin_num,t2.order_money,t3.is_pay_user
    '''
    print pay_coin_distri_sql
    pay_coin_distri_df = hql_to_df(pay_coin_distri_sql).fillna(0)
    print pay_coin_distri_df.head()

    result_df = pay_coin_distri_df.merge(is_new_old_server_df, on=['ds','server_id'], how='left')
    result_df = result_df.groupby(['ds', 'is_not_new','is_pay_user',]).agg({
        'coin_num': lambda g: g.sum(),
        'order_money': lambda g: g.sum(),
        'user_id': lambda g: g.nunique(),
    }).reset_index()
    result_df = result_df.rename(columns={'user_id': 'user_num', })

    result_1_df = act_info_df.merge(is_new_old_server_df, on=['ds', 'server_id'], how='left')
    result_1_df = result_1_df.groupby(['ds', 'is_not_new',]).agg({
        'dau': lambda g: g.sum(),
    }).reset_index()

    result_df = result_df.merge(result_1_df, on=['ds', 'is_not_new'], how='left')
    print result_df.head()

    result_df.to_excel(r'E:\sanguo_ks_new_old_server-20170828.xlsx', index=False)

if __name__ == '__main__':
    for platform in ['sanguo_ks',]:
        settings_dev.set_env(platform)
        data_reduce()
    print "end"