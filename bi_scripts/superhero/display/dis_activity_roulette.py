#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Zhang Yongchen
Description : 转盘由8个奖励组成，启动转盘需消耗钻石，转盘刷新功能可刷新钻盘中的奖励；启动一次转盘消耗50钻石，启动十次转盘消耗480钻石；启动钻石会返回积分，根据积分排名获得相应奖励。
roulette关键字，单转一次转盘roulette.open_roulette，十连转roulette.open_roulette10；
'''
from utils import get_active_conf, hql_to_df, ds_short, update_mysql, date_range
import settings_dev
import pandas as pd

def dis_actiivty_roulette(date):

    version, act_start_time, act_end_time = get_active_conf('roulette', date)

    if version != '':
        act_start_short = ds_short(act_start_time)
        act_end_short = ds_short(act_end_time)
        print version, act_start_time, act_end_time
        dis_actiivty_roulette_bi(act_start_short,act_end_short,act_start_time,act_end_time)
    else:
        print '{0} 没有转盘活动'.format(date)


def dis_actiivty_roulette_bi(act_start_short,act_end_short,act_start_time,act_end_time):
    if settings_dev.code == 'superhero_bi':
        for plat in ['superhero_pub', 'superhero_ios']:
            print plat
            dis_actiivty_roulette_one(act_start_short, act_end_short, act_start_time, act_end_time, plat)
    else:
        dis_actiivty_roulette_one(act_start_short, act_end_short, act_start_time, act_end_time)


def bi_sql(act_start_short, act_end_short, act_start_time, act_end_time, pp):

    # action信息
    active_sql_1 = '''
           select uid, action from raw_action_log where ds>='{act_start_short}' and ds<='{act_end_short}' and act_time>='{act_start_time}' and act_time<='{act_end_time}'
           and action = 'roulette.open_roulette' and substr(uid,1,1) = '{pp}'
       '''.format(act_start_short=act_start_short, act_end_short=act_end_short, act_start_time=act_start_time, act_end_time=act_end_time, pp=pp)

    active_sql_10 = '''
           select uid, action from raw_action_log where ds>='{act_start_short}' and ds<='{act_end_short}' and act_time>='{act_start_time}' and act_time<='{act_end_time}'
           and action = 'roulette.open_roulette10' and substr(uid,1,1) = '{pp}'
       '''.format(act_start_short=act_start_short, act_end_short=act_end_short, act_start_time=act_start_time, act_end_time=act_end_time, pp=pp)

    # 用户信息
    info_sql = '''
          select uid, reverse(substr(reverse(uid),8)) as server, max(vip_level) as vip_level from raw_info where ds >= '{act_start_short}' and ds<='{act_end_short}'
          and substr(uid,1,1) = '{pp}' group by uid, server
      '''.format(act_start_short=act_start_short, act_end_short=act_end_short, pp=pp)
    # print info_sql

    # 消费信息
    spend_sql = '''
           select uid, sum(coin_num) as spend from raw_spendlog where ds >= '{act_start_short}' and ds<='{act_end_short}' and subtime>='{act_start_time}' and subtime<='{act_end_time}'
           and goods_type like '%roulette%' and substr(uid,1,1) = '{pp}' group by uid
       '''.format(act_start_short=act_start_short, act_end_short=act_end_short, act_start_time=act_start_time, act_end_time=act_end_time, pp=pp)

    # 充值信息
    pay_sql = '''
           select uid, sum(order_money) as money from raw_paylog where ds >= '{act_start_short}' and ds<='{act_end_short}' and substr(uid,1,1) = '{pp}' group by uid
       '''.format(act_start_short=act_start_short, act_end_short=act_end_short, pp=pp)
    # print pay_sql

    return active_sql_1, active_sql_10, info_sql, spend_sql, pay_sql


def dis_actiivty_roulette_one(act_start_short, act_end_short, act_start_time, act_end_time, plat=None):

    #action信息

    active_sql_1 = '''
            select uid, action from raw_action_log where ds>='{act_start_short}' and ds<='{act_end_short}' and act_time>='{act_start_time}' and act_time<='{act_end_time}'
            and action = 'roulette.open_roulette'
        '''.format(act_start_short=act_start_short, act_end_short=act_end_short, act_start_time=act_start_time, act_end_time=act_end_time)

    active_sql_10 = '''
            select uid, action from raw_action_log where ds>='{act_start_short}' and ds<='{act_end_short}' and act_time>='{act_start_time}' and act_time<='{act_end_time}'
            and action = 'roulette.open_roulette10'
        '''.format(act_start_short=act_start_short, act_end_short=act_end_short, act_start_time=act_start_time, act_end_time=act_end_time)

    # 用户信息
    info_sql = '''
           select uid, reverse(substr(reverse(uid),8)) as server, max(vip_level) as vip_level from raw_info where ds >= '{act_start_short}' and ds<='{act_end_short}'
           group by uid, server
       '''.format(act_start_short=act_start_short, act_end_short=act_end_short)

    #消费信息
    spend_sql = '''
        select uid, sum(coin_num) as spend from raw_spendlog where ds >= '{act_start_short}' and ds<='{act_end_short}' and subtime>='{act_start_time}' and subtime<='{act_end_time}'
        and goods_type like '%roulette%' group by uid
    '''.format(act_start_short=act_start_short, act_end_short=act_end_short, act_start_time=act_start_time, act_end_time=act_end_time)

    #充值信息
    pay_sql = '''
        select uid, sum(order_money) as money from raw_paylog where ds >= '{act_start_short}' and ds<='{act_end_short}' group by uid
    '''.format(act_start_short=act_start_short, act_end_short=act_end_short)

    #根据plat更新sql
    if plat == 'superhero_pub':
        active_sql_1, active_sql_10, info_sql, spend_sql, pay_sql = bi_sql(act_start_short, act_end_short, act_start_time, act_end_time, 'g')
    if plat == 'superhero_ios':
        active_sql_1, active_sql_10, info_sql, spend_sql, pay_sql = bi_sql(act_start_short, act_end_short, act_start_time, act_end_time, 'a')
    if settings_dev.platform == 'superhero_vt':
        pay_sql = '''
            select uid, sum(order_rmb) as money from raw_paylog where ds >= '{act_start_short}' and ds<='{act_end_short}' group by uid
        '''.format(act_start_short=act_start_short, act_end_short=act_end_short)
        # print pay_sql

    #sql转Dataframe
    active_df_1 = hql_to_df(active_sql_1)
    active_df_1['core'] = 10
    active_df_10 = hql_to_df(active_sql_10)
    active_df_10['core'] = 110
    active_df = pd.concat([active_df_1, active_df_10])
    active_df = active_df.groupby('uid').agg({
        'action': lambda g: g.count(),
        'core': lambda g: g.sum()
    }).rename(columns={'action': 'times'}).reset_index()

    # print active_df.head(10)
    info_df = hql_to_df(info_sql)
    # print info_df.head(10)
    spend_df = hql_to_df(spend_sql)
    # print spend_df.head(10)
    pay_df = hql_to_df(pay_sql)
    # print pay_df.head(10)

    #合并数据
    result_df = active_df.merge(info_df, on='uid', how='left').fillna(0)
    # print result_df.dtypes
    result_df = result_df.merge(spend_df, on='uid', how='left').fillna(0)
    # print result_df.dtypes
    result_df = result_df.merge(pay_df, on='uid', how='left').fillna(0)
    # print result_df.head(10)
    #全服信息
    server_df = result_df.groupby('server').sum().spend.reset_index().rename(columns={'spend':'server_spend'})
    # print server_df.head(10)
    result_df = result_df.merge(server_df, on='server', how='left')
    result_df = result_df.sort_values(by=['core', 'spend'], ascending=False)[0:500]
    result_df['ds'] = act_start_short
    result_df['rank'] = range(1, (len(result_df) + 1))
    columns = ['ds', 'rank', 'uid', 'server', 'vip_level', 'times', 'core', 'money', 'spend', 'server_spend']
    result_df = result_df[columns]
    # print result_df.head(10)

    # 更新MySQL表
    table = 'dis_activity_roulette'
    del_sql = 'delete from {0} where ds="{1}"'.format(table, act_start_short)
    update_mysql(table, result_df, del_sql, plat)
    print act_start_short, table, 'complete'


if __name__ == '__main__':
    for platform in ('superhero_bi', 'superhero_vt', 'superhero_qiku', 'superhero_tw', 'superhero_self_en'):
        settings_dev.set_env(platform)
        for date in date_range('20170113', '20170116'):
            dis_actiivty_roulette(date)
