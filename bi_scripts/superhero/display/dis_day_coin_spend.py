#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Dong Junshuang
Description : 营收 - 每日钻石消费
Time        : 2017.04.28
illustration:
'''
import settings_dev
from utils import ds_add
from utils import hqls_to_dfs,hql_to_df,date_range
from utils import update_mysql
from sqls_for_games.superhero import gs_sql
import pandas as pd

def dis_day_coin_spend(date):
    assist_sql = '''
    SELECT ds,
        vip as vip_level,
        plat,
        sum(order_coin) AS pay_get_coin,
        sum(spend_coin) AS coin_spend
    FROM mart_assist
    WHERE ds = '{date}'
    GROUP BY ds,vip,plat
    '''.format(date=date)
    info_sql = '''
    SELECT ds,
        uid as user_id,
        vip_level,
        substr(uid,1,1) AS plat,
        zuanshi
    FROM mid_info_all
    WHERE ds >= '{date_ago}'
    AND ds <= '{date}'
    '''.format(date=date, date_ago=ds_add(date, -1))

    assist_df = hql_to_df(assist_sql).fillna(0)
    assist_df = pd.DataFrame(assist_df).drop_duplicates()
    print assist_sql
    print assist_df

    info_df = hql_to_df(info_sql).fillna(0)
    print info_sql
    print info_df

    gs_df = hql_to_df(gs_sql).fillna(0)
    print gs_sql
    print gs_df
    # assist_df, info_df, gs_df = hqls_to_dfs([assist_sql, info_sql, info_sql])

    sum_dic = {ds_add(date, -1): 'yes_coin', date: 'coin_save'}
    # 排除开服至今的gs数据
    info_df = info_df[~info_df['user_id'].isin(gs_df.user_id.values)]
    sum_result_df = (info_df.groupby(['ds', 'vip_level', 'plat'])
                     .sum().zuanshi.reset_index()
                     .pivot_table('zuanshi', ['vip_level', 'plat'], 'ds')
                     .reset_index().rename(columns=sum_dic).fillna(0))
    if date == settings_dev.start_date.strftime('%Y%m%d'):
        sum_result_df['yes_coin'] = 0
    result_df = (sum_result_df.merge(assist_df,
                                     on=['vip_level', 'plat'],
                                     how='outer').fillna(0))
    result_df['new_coin'] = result_df['coin_save'] - result_df['yes_coin']
    result_df['free_get_coin'] = result_df['coin_spend'] - \
        result_df['pay_get_coin'] + result_df['new_coin']
    result_df['ds'] = date
    print result_df.head()
    dau_coin_sql = '''
        select t1.vip_level,t2.plat,count(distinct t1.uid) as dau,sum(t1.coin_num) as dau_coin_num from (
            select uid,vip_level,zuanshi as coin_num   from raw_info where ds='{date}' group by uid,vip_level,zuanshi
        )t1 left outer join(
          select user_id as uid,plat from mart_assist where ds='{date}' group by uid,plat
        )t2 on t1.uid = t2.uid
        group by  t1.vip_level,t2.plat
    '''.format(date=date)
    dau_coin_df = hql_to_df(dau_coin_sql).fillna(0)
    print dau_coin_df.head()
    result_df = result_df.merge(dau_coin_df, on=['vip_level','plat'], how='left')
    print result_df.head()

    # 更新MySQL
    table = 'dis_day_coin_spend_new'
    del_sql = 'delete from {0} where ds="{1}"'.format(table, date)
    column = ['ds', 'vip_level', 'new_coin', 'free_get_coin', 'pay_get_coin',
              'coin_save', 'coin_spend','dau','dau_coin_num',]

    if settings_dev.code == 'superhero_bi':
        pub_result_df = result_df[result_df.plat == 'g'][column]
        ios_result_df = result_df[result_df.plat == 'a'][column]
        update_mysql(table, pub_result_df, del_sql, 'superhero_pub')
        update_mysql(table, ios_result_df, del_sql, 'superhero_ios')
    else:
        update_mysql(table, result_df[column], del_sql)

    print '{0} complete'.format(table)


if __name__ == '__main__':
    res_list = []
    settings_dev.set_env('superhero_qiku')
    for date in date_range('20170801', '20170824'):
        print date
        dis_day_coin_spend(date)
    print 'end'
