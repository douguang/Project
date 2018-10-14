#!/usr/bin/env python  
# encoding: utf-8  

""" 
@author: Andy 
@site:  赏金令活动需求
@software: PyCharm 
@file: superhero_vt_bounty_order.py
@time: 17/9/4 下午6:13 
"""

import settings_dev
from utils import ds_add
from utils import hql_to_df
from utils import update_mysql
from sqls_for_games.superhero import gs_sql
import pandas as pd


def data_reduce(date):

    info_sql = '''
    select t5.ds,t5.vip,t5.dangci,count(distinct t5.account) as num from (
       select t1.ds,t1.vip,t1.account,case when t2.order_coin<=500 then '500' when (t2.order_coin>500  and t2.order_coin<=3000) then '500-3000' when (t2.order_coin>3000  and t2.order_coin<=5000) then '3000-5000' else '5000' end as dangci from
       (
       select ds,account,max(vip_level) as vip from raw_info where ds='{date}' and uid in (
            select uid from raw_paylog where ds='{date}' and platform_2 != 'admin' and platform_2 != 'admin_test' group by uid
          )
          group by ds,account
      )t1 left outer join(
        select t3.ds,t3.account,sum(t4.order_coin) as order_coin from (
          select ds,account,uid from raw_info where ds='{date}' group by ds,account,uid
          )t3 left outer join(
            select ds,uid,sum(order_coin) as order_coin from raw_paylog where ds='{date}' and platform_2 != 'admin' and platform_2 != 'admin_test' group by ds,uid
            )t4 on (t3.ds=t4.ds and t3.uid=t4.uid)
        group by t3.ds,t3.account
        )t2 on (t1.ds=t2.ds and t1.account=t2.account)
        group by t1.ds,t1.vip,t1.account,dangci
       )t5 
       where t5.vip != 0
       group by t5.ds,t5.vip,t5.dangci
    '''.format(date=date,)
    print info_sql
    info_df = hql_to_df(info_sql)
    print info_df.head(3)
    info_df.to_excel('/Users/kaiqigu/Documents/Sanguo/超级英雄-越南-赏金令-1_%s.xlsx' % date)

    ###################################################################
    info_2_sql = '''
        select t5.ds,t5.vip,t5.dangci,count(distinct t5.account) as num from (
           select t1.ds,t1.vip,t1.account,case when t2.order_coin<=1000 then '1000' when (t2.order_coin>1000  and t2.order_coin<=2000) then '1000-2000' when (t2.order_coin>2000  and t2.order_coin<=3000) then '2000-3000' else '3000' end as dangci from
           (
           select ds,account,max(vip_level) as vip from raw_info where ds='{date}' and uid in (
                select uid from raw_paylog where ds='{date}' and platform_2 != 'admin' and platform_2 != 'admin_test' group by uid
              )
              group by ds,account
          )t1 left outer join(
            select t3.ds,t3.account,sum(t4.order_coin) as order_coin from (
              select ds,account,uid from raw_info where ds='{date}' group by ds,account,uid
              )t3 left outer join(
                select ds,uid,sum(order_coin) as order_coin from raw_paylog where ds='{date}' and platform_2 != 'admin' and platform_2 != 'admin_test' group by ds,uid
                )t4 on (t3.ds=t4.ds and t3.uid=t4.uid)
            group by t3.ds,t3.account
            )t2 on (t1.ds=t2.ds and t1.account=t2.account)
            group by t1.ds,t1.vip,t1.account,dangci
           )t5 
           where t5.vip != 0
           group by t5.ds,t5.vip,t5.dangci
        '''.format(date=date, )
    print info_2_sql
    info_2_df = hql_to_df(info_2_sql)
    print info_2_df.head(3)
    info_2_df.to_excel('/Users/kaiqigu/Documents/Sanguo/超级英雄-越南-赏金令-2_%s.xlsx' % date)

    ###################################################################
    account_uid_sql = '''
                select ds,account,uid from raw_info where ds='{date}' group by ds,account,uid
            '''.format(date=date, )
    account_uid_df = hql_to_df(account_uid_sql)
    print account_uid_df.head(3)

    account_max_level_sql = '''
                select account,max(vip_level) as vip from raw_info where ds='{date}' group by account
            '''.format(date=date, )
    account_max_level_df = hql_to_df(account_max_level_sql)
    print account_max_level_df.head(3)
    account_uid_df = account_uid_df.merge(account_max_level_df, on=['account', ], how='left')
    print account_uid_df.head()

    dau_df = account_uid_df.groupby(['ds', 'vip']).agg({'account': lambda g: g.nunique(), }).reset_index()
    dau_df = dau_df.rename(columns={'account': 'dau', })
    print dau_df.head()

    canyu_sql = '''
            select ds,uid  from raw_action_log where ds='{date}' and action='bounty_order.get_big_reward' group by ds,uid
        '''.format(date=date, )
    canyu_df = hql_to_df(canyu_sql)
    print canyu_df.head(3)
    canyu_df = canyu_df.merge(account_uid_df, on=['ds', 'uid', ], how='left')
    print '---'
    print canyu_df
    canyu_df = canyu_df.groupby(['ds', 'vip']).agg({'account': lambda g: g.nunique(), }).reset_index()
    canyu_df = canyu_df.rename(columns={'account': 'canyuNum', })
    print canyu_df.head()

    order_coin_sql = '''
            select ds,uid,sum(order_coin) as order_coin from raw_paylog where ds='{date}' and platform_2 != 'admin'  and platform_2 != 'admin_test'  group by ds,uid
        '''.format(date=date, )
    order_coin_df = hql_to_df(order_coin_sql)
    print order_coin_df.head(3)
    order_coin_df = order_coin_df.merge(account_uid_df, on=['ds', 'uid', ], how='left')
    order_coin_df = order_coin_df.groupby(['ds', 'vip']).agg(
        {'account': lambda g: g.nunique(), 'order_coin': lambda g: g.sum(), }).reset_index()
    order_coin_df = order_coin_df.rename(columns={'account': 'chognzhirenNum', })
    print order_coin_df.head()

    info_3_df = dau_df.merge(canyu_df, on=['ds', 'vip', ], how='left')
    info_3_df = info_3_df.merge(order_coin_df, on=['ds', 'vip', ], how='left')

    info_3_df.to_excel('/Users/kaiqigu/Documents/Sanguo/超级英雄-越南-赏金令-0_%s.xlsx' % date)

if __name__ == '__main__':
    settings_dev.set_env('superhero_vt')
    date = '20170903'
    result = data_reduce(date)
