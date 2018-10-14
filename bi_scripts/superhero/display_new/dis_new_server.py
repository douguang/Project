#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Dong Junshuang
Description : 用户 - 新服数据
Time        : 2017.05.05
illustration:
'''
import settings_dev
import pandas as pd
from utils import ds_add
from utils import format_dates
from utils import hql_to_df
from utils import hqls_to_dfs
from utils import update_mysql
from sqls_for_games.superhero import gs_sql

keep_days = [2, 3, 7]


def dis_new_server_keep_rate(date):
    '''在某一天执行'''
    server_start_date = settings_dev.start_date.strftime('%Y%m%d')
    # 要抓取的注册日期
    reg_dates = filter(lambda d: d >= server_start_date, [
        ds_add(date, 1 - d) for d in keep_days
    ])
    if not reg_dates:
        return
    # 要抓取的活跃日期
    act_dates = set()
    for reg_date in reg_dates:
        act_dates.add(reg_date)
        for keep_day in keep_days:
            act_dates.add(ds_add(reg_date, keep_day - 1))

    reg_sql = '''
    SELECT ds as reg_ds,
           uid,
           reverse(substring(reverse(uid), 8)) AS server,
           substr(uid,1,1) as plat
    FROM raw_reg
    WHERE ds IN {reg_dates}
    '''.format(reg_dates=format_dates(reg_dates))
    act_sql = '''
    SELECT ds AS act_ds,
           uid,
           reverse(substring(reverse(uid), 8)) AS server,
           substr(uid,1,1) AS plat
    FROM raw_act
    WHERE ds IN {act_dates}
    '''.format(act_dates=format_dates(act_dates))
    reg_df, act_df, gs_df = hqls_to_dfs([reg_sql, act_sql, gs_sql])
    # 排除开服至今的gs数据
    gs_df = gs_df.rename(columns={'user_id': 'uid'})
    reg_df = reg_df[~reg_df['uid'].isin(gs_df.uid.values)]
    act_df = act_df[~act_df['uid'].isin(gs_df.uid.values)]
    reg_df['reg'] = 1
    # 活跃用户表转职后合并到注册表后，将活跃日期变成列名
    act_df['act'] = 1
    reg_act_df = (
        act_df.pivot_table('act', ['uid', 'server', 'plat'], 'act_ds')
        .reset_index().merge(reg_df,
                             on=['uid', 'server', 'plat'],
                             how='right').reset_index())

    # 求每一个受影响的日期留存率，然后合并
    keep_rate_dfs = []
    for reg_date in reg_dates:
        act_dates = [ds_add(reg_date, keep_day - 1) for keep_day in keep_days]
        act_dates_dic = {ds_add(reg_date, keep_day - 1): 'd%d_keep' % keep_day
                         for keep_day in keep_days}
        keep_df = (
            reg_act_df.loc[reg_act_df.reg_ds == reg_date,
                           ['reg', 'server', 'plat'] + act_dates_dic.keys()]
            .rename(columns=act_dates_dic).groupby(['server', 'plat']).sum()
            .reset_index().fillna(0))
        for c in act_dates_dic.values():
            keep_df[c + 'rate'] = keep_df[c] / keep_df.reg
        keep_df['ds'] = reg_date
        keep_rate_dfs.append(keep_df)

    keep_rate_df = pd.concat(keep_rate_dfs)
    result_df = keep_rate_df.rename(columns={'reg': 'reg_user_num'})
    print result_df

    # 更新MySQL表
    table = 'dis_new_server_keep_rate'
    del_sql = 'delete from {0} where ds="{1}"'.format(table, date)
    column = ['ds', 'server', 'reg_user_num'] + \
        ['d%d_keeprate' % d for d in keep_days]
    if settings_dev.code == 'superhero_bi':
        pub_result_df = result_df[result_df.plat == 'g'][column]
        ios_result_df = result_df[result_df.plat == 'a'][column]
        update_mysql(table, pub_result_df, del_sql, 'superhero_pub')
        update_mysql(table, ios_result_df, del_sql, 'superhero_ios')
    else:
        update_mysql(table, result_df[column], del_sql)

    print '{0} is complete'.format(table)


def dis_new_server(date):
    assist_sql = '''
    SELECT ds,
           server,
           plat,
           SUM(CASE WHEN vip >0 THEN 1 ELSE 0 END) AS vip_num,
           SUM(CASE WHEN vip = 0 THEN 1 ELSE 0 END) AS vip0_num,
           SUM(CASE WHEN order_money >0 THEN 1 ELSE 0 END) AS pay_num,
           SUM(order_money) AS sum_money,
           SUM(CASE WHEN order_money = 6 THEN 1 ELSE 0 END) AS pay6_num,
           SUM(is_new_user) AS reg_user_num,
           SUM(user_id) AS act_num,
           SUM(CASE WHEN order_money >0 THEN 1 ELSE 0 END)/COUNT(user_id) AS pay_rate,
           SUM(order_money)/COUNT(user_id) AS arpu,
           SUM(order_money)/SUM(CASE WHEN order_money >0 THEN 1 ELSE 0 END) AS arppu
    FROM mart_assist
    WHERE ds = '{date}'
    GROUP BY ds,
             server,
             plat
    '''.format(date=date)
    result_df = hql_to_df(assist_sql)

    # 更新MySQL表
    table = 'dis_new_server'
    del_sql = 'delete from {0} where ds="{1}"'.format(table, date)
    column = ['ds', 'server', 'vip_num', 'vip0_num', 'pay_num', 'sum_money',
              'pay6_num', 'reg_user_num', 'act_num', 'pay_rate', 'arpu',
              'arppu']
    if settings_dev.code == 'superhero_bi':
        pub_result_df = result_df[result_df.plat == 'g'][column]
        ios_result_df = result_df[result_df.plat == 'a'][column]
        update_mysql(table, pub_result_df, del_sql, 'superhero_pub')
        update_mysql(table, ios_result_df, del_sql, 'superhero_ios')
    else:
        update_mysql(table, result_df[column], del_sql)

    print '{0} is complete'.format(table)
    # 调用分服留存率
    dis_new_server_keep_rate(date)


if __name__ == '__main__':
    settings_dev.set_env('superhero_bi')
    date = '20170501'
    dis_new_server(date)
