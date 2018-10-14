#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@Time    :
@Author  :
@File    :
@Software: PyCharm
Description :  流失、留存用户的等级分布、新手引导停留、付费分布、钻石获取与消耗
"""
import settings_dev
from utils import hql_to_df, format_date, ds_add, hqls_to_dfs, date_range
import pandas as pd


def loss_user(date, del_ds):
    regtime = format_date(date)
    print regtime
    loss_sql = '''
        select '{regtime}' as regtime, user_id, 'yes' as loss from raw_info where ds='{date}' and to_date(reg_time)='{regtime}' and user_id not in
        (select user_id from raw_info where ds='{date_3}')
    '''.format(date=date, date_3=ds_add(date, int(del_ds)), regtime=regtime)
    print loss_sql
    keep_sql = '''
        select '{regtime}' as regtime, user_id, 'no' as loss from raw_info where ds='{date}' and to_date(reg_time)='{regtime}' and user_id in
        (select user_id from raw_info where ds='{date_3}')
    '''.format(date=date, date_3=ds_add(date, int(del_ds)), regtime=regtime)
    print keep_sql
    loss_df = hql_to_df(loss_sql)
    keep_df = hql_to_df(keep_sql)
    user_df = pd.concat([loss_df, keep_df])
    print user_df.head(5)
    return user_df


def level_df(date, del_ds):
    regtime = format_date(date)
    sql = '''
        select user_id, level from mid_info_all where ds='{0}' and to_date(reg_time)='{1}'
    '''.format(ds_add(date, del_ds), regtime)
    print sql
    df = hql_to_df(sql)
    print df.head(5)
    return df


def pay_df(date, del_ds):
    sql = '''
        select user_id, sum(order_rmb) as pay from raw_paylog where ds>='20170706' and ds<='{date}' and status=1 group by user_id
    '''.format(date=ds_add(date, del_ds))
    print sql
    df = hql_to_df(sql)
    print df.head(5)
    return df

def coin_df(date, del_ds):
    acqure_sql = '''
        select user_id, sum(coin_diff) as acqure_coin from parse_actionlog where ds>='20170706' and ds<='{date}' and coin_diff>0 and a_typ!='player_reconnect_req' group by user_id
    '''.format(date=ds_add(date, del_ds))
    print acqure_sql
    consume_sql = '''
        select user_id, sum(coin_diff) as consume_coin from parse_actionlog where ds>='20170706' and ds<='{date}' and coin_diff<0 group by user_id
    '''.format(date=ds_add(date, del_ds))
    print consume_sql
    acqure_df = hql_to_df(acqure_sql)
    consume_df = hql_to_df(consume_sql)
    coin_df = acqure_df.merge(consume_df, on='user_id', how='outer')
    print coin_df.head(5)
    return coin_df


def guide_df(date, del_ds):
    sql = '''
    select * from (
        select user_id, guide_done, row_number() over(partition by user_id order by ds desc) as rn from raw_guide where ds>='20170706' and ds<='{date}') t1
    where rn = 1
    '''.format(date=ds_add(date, del_ds))
    print sql
    df = hql_to_df(sql)
    user_id, guide = [], []
    for _, row in df.iterrows():
        guide_done = eval(row['guide_done'])
        guide_id = 0
        try:
            guide_id = max(guide_done)
        except:
            pass
        guide.append(guide_id)
        user_id.append(row['user_id'])
    guide_df = pd.DataFrame({'user_id': user_id, 'guide': guide})
    print guide_df.head(5)
    return guide_df


if __name__ == '__main__':
    settings_dev.set_env('jianniang_tw')
    result_list = []
    for date in date_range('20170708', '20170708'):
        user_df = loss_user(date, 3)
        level_df = level_df(date, 3)
        pay_df = pay_df(date, 3)
        coin_df = coin_df(date, 3)
        guide_df = guide_df(date, 3)
        result_df = user_df.merge(level_df, on='user_id', how='left')\
            .merge(pay_df, on='user_id', how='left')\
            .merge(coin_df, on='user_id', how='left')\
            .merge(guide_df, on='user_id', how='left').fillna(0)
        result_df.to_excel(r'E:\Data\output\H5\loss_user_%s.xlsx'%date)
        result_list.append(result_df)
    result = pd.concat(result_list)
    result.to_excel(r'E:\Data\output\H5\loss_user.xlsx')
