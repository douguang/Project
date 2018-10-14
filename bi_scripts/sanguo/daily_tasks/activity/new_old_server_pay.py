#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
@Time    : 17-7-13 下午2:14
@Author  : Andy 
@File    : new_old_server_pay.py
@Software: PyCharm
Description :
'''


import calendar
import settings_dev
from utils import ds_add, hql_to_df, update_mysql, hqls_to_dfs, date_range


def data_reduce(date):
    # 日活
    act_info_sql = '''
        select ds,reverse(substring(reverse(user_id),8)) as server_id,count(distinct user_id) as dau from raw_info where ds='{date}' group by ds,server_id
    '''.format(**{'date': date,})
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

    # 充值钻石档次
    pay_coin_distri_sql = '''
    select t1.ds,t1.user_id,reverse(substring(reverse(t1.user_id),8)) as server_id,
        case when t1.order_coin<=1600 then 'pay_coin_1600'
                 when t1.order_coin> 1601 and t1.order_coin<=8000 then 'pay_coin_1601_8000'
                  when t1.order_coin>8000 and t1.order_coin<=20000 then 'pay_coin_8001_20000'
                     when t1.order_coin>20000  then 'pay_coin_20001'
                 else 'g' end as pay_coin_level
        from (
        select ds,user_id,sum(order_coin) as order_coin from raw_paylog where ds='{date}' and platform_2 != 'admin_test'  group by ds,user_id
    )t1
    group by t1.ds,t1.user_id,server_id,pay_coin_level
    '''.format(**{'date': date,})
    print pay_coin_distri_sql
    pay_coin_distri_df = hql_to_df(pay_coin_distri_sql)
    print pay_coin_distri_df.head()

    # 上古神兵购买箱子消耗的钻石数 上古神兵参加的人数
    group_buy_sql = '''
    select ds,reverse(substring(reverse(user_id),8)) as server_id,sum(coin_num) as group_coin_num,count(distinct user_id) as num from raw_spendlog where ds='{date}' and goods_type like '%group_buy%' group by  ds,server_id
    '''.format(**{'date': date,})
    print group_buy_sql
    group_buy_df = hql_to_df(group_buy_sql)
    print group_buy_df.head()

    result_1_df = act_info_df.merge(is_new_old_server_df, on=['ds','server_id'], how='left')
    result_1_df = result_1_df.groupby(['ds', 'is_not_new',]).agg({
        'dau': lambda g: g.sum(),
    }).reset_index()

    result_2_df = pay_coin_distri_df.merge(is_new_old_server_df, on=['ds','server_id'], how='left')
    result_2_df = result_2_df.groupby(['ds','is_not_new','pay_coin_level',]).agg({
        'user_id': lambda g: g.nunique(),
    }).reset_index().rename(columns={'user_id': 'pay_num',})

    result_3_df = group_buy_df.merge(is_new_old_server_df, on=['ds', 'server_id'], how='left')
    result_3_df = result_3_df.groupby(['ds', 'is_not_new', ]).agg({
        'group_coin_num': lambda g: g.sum(),
        'num': lambda g: g.sum(),
    }).reset_index()

    result_df = result_1_df.merge(result_2_df, on=['ds', 'is_not_new', ], how='left')
    result_df.to_excel(r'/home/kaiqigu/桌面/机甲无双-多语言-无上古神兵活动效果数据_%s.xlsx' % date, index=False)
    result_df = result_df.merge(result_3_df, on=['ds', 'is_not_new', ], how='left')

    return result_df


if __name__ == '__main__':
    #dis_kaiqigu_pay_detail('20170102')
    for platform in ['sanguo_tl',]:
        settings_dev.set_env(platform)
        date = '20170710'
        result = data_reduce(date)
        result.to_excel(r'/home/kaiqigu/桌面/机甲无双-多语言-上古神兵活动效果数据_%s.xlsx' % date, index=False)
    print "end"