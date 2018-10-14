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
from utils import date_range

keep_days = [2, 3, 7]


def dis_new_server_keep_rate(excute_date):
    '''在某一天执行'''
    server_start_date = settings_dev.start_date.strftime('%Y%m%d')
    # 要抓取的注册日期
    reg_dates = filter(lambda d: d >= server_start_date,
                       [ds_add(excute_date, 1 - d) for d in keep_days])
    # 要抓取的活跃日期
    act_dates = set()
    for reg_date in reg_dates:
        act_dates.add(reg_date)
        for keep_day in keep_days:
            act_dates.add(ds_add(reg_date, keep_day - 1))

    reg_sql = '''
    SELECT  ds,
            uid as user_id,
            substr(uid,1,1) as plat
       FROM raw_reg
       WHERE ds IN {reg_dates}
    '''.format(reg_dates=format_dates(reg_dates))

    reg_act_sql = '''
    SELECT  ds,
            uid as user_id,
            substr(uid,1,1) as plat
       FROM raw_reg
       WHERE ds IN {act_dates}
    '''.format(act_dates=format_dates(act_dates))

    reg_raw_df, reg_act_df, gs_df = hqls_to_dfs([reg_sql, reg_act_sql, gs_sql])
    reg_raw_df['reg'] = 1

    act_sql = '''
    SELECT ds,
           uid as user_id,
           reverse(substr(reverse(uid), 8)) AS server,
           substr(uid,1,1) as plat
    FROM raw_info
    WHERE ds IN {act_dates}
    '''.format(act_dates=format_dates(act_dates))
    act_df = hql_to_df(act_sql)

    # 排除开服至今的测试用户的数据
    act_df = act_df[~act_df['user_id'].isin(gs_df.user_id.values)]
    reg_raw_df = reg_raw_df[~reg_raw_df['user_id'].isin(gs_df.user_id.values)]
    reg_act_df = reg_act_df[~reg_act_df['user_id'].isin(gs_df.user_id.values)]

    reg_df = reg_raw_df.merge(act_df,
                              on=['ds', 'user_id', 'plat'],
                              how='left').fillna('None')

    dau_df = reg_act_df.merge(act_df,
                              on=['ds', 'user_id', 'plat'],
                              how='outer').fillna('None')

    # 活跃用户表转职后合并到注册表后，将活跃日期变成列名
    act_df['act'] = 1
    reg_act_df = (
        act_df.pivot_table('act', ['user_id', 'server', 'plat'], 'ds')
        .reset_index().merge(reg_df,
                             on=['user_id', 'server', 'plat'],
                             how='right').reset_index())

    # dau
    dau_result = dau_df.groupby(
        ['ds', 'plat', 'server']).count().reset_index().rename(columns={
            'user_id': 'dau',
        })

    # 求每一个受影响的日期留存率，然后合并
    keep_rate_dfs = []
    for reg_date in reg_dates:
        act_dates = [ds_add(reg_date, keep_day - 1) for keep_day in keep_days]
        act_dates_dic = {ds_add(reg_date, keep_day - 1): 'd%d_keep' % keep_day
                         for keep_day in keep_days}
        keep_df = (reg_act_df.loc[reg_act_df.ds == reg_date, [
            'reg', 'server', 'plat'
        ] + act_dates_dic.keys()].rename(columns=act_dates_dic).groupby(
            ['server', 'plat']).sum().reset_index().fillna(0))
        result = dau_result.loc[dau_result.ds == reg_date].merge(
            keep_df, on=['plat', 'server'],
            how='outer').fillna(0)
        for c in act_dates_dic.values():
            result[c + 'rate'] = result[c] / result.reg
        result = result.fillna(0)
        keep_rate_dfs.append(result)

    keep_rate_df = pd.concat(keep_rate_dfs)
    columns = ['ds', 'reg', 'plat', 'server'] + ['d%d_keeprate' % d
                                                 for d in keep_days]
    result_df = keep_rate_df[columns]
    rename_dic = {'reg': 'reg_user_num'}
    result_df = result_df.rename(columns=rename_dic)
    # print result_df

    # 更新MySQL
    table = 'dis_new_server_keep_rate'
    del_sql = 'delete from {0} where ds in {1}'.format(table,
                                                       format_dates(reg_dates))
    column = ['ds', 'server', 'reg_user_num'] + ['d%d_keeprate' % d
                                                 for d in keep_days]

    if settings_dev.code == 'superhero_bi':
        pub_result_df = result_df[result_df.plat == 'g'][column]
        ios_result_df = result_df[result_df.plat == 'a'][column]
        update_mysql(table, pub_result_df, del_sql, 'superhero_pub')
        update_mysql(table, ios_result_df, del_sql, 'superhero_ios')
    else:
        update_mysql(table, result_df[column], del_sql)

    print '{0} complete'.format(table)


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
           COUNT(user_id) AS act_num,
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
    result_df = pd.DataFrame(result_df).drop_duplicates()
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
    for date in date_range('20170401', '20170511'):
        print date
        dis_new_server(date)
