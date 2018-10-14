#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Zhang Yongchen
Description : 活动期间抽取武将获得积分，活动结束后按照积分排名进行奖励发放；抽取武将分为单抽和十连抽，单抽10积分、十连抽110积分。
gacha关键字，抽奖为gacha.get_gacha，单抽返回值为1、2,十连抽为3、4、7、8,1、4没分数，2为10分，其余110
'''
from utils import get_active_conf, hql_to_df, ds_short, update_mysql, date_range
import settings_dev
import pandas as pd

def dis_actiivty_limit_hero(date):

    version, act_start_time, act_end_time = get_active_conf('limit_hero_score', date)

    if version != '':
        act_start_short = ds_short(act_start_time)
        act_end_short = ds_short(act_end_time)
        print version, act_start_time, act_end_time
        dis_actiivty_limit_hero_bi(act_start_short,act_end_short,act_start_time,act_end_time)
    else:
        print '{0} 没有限时神将活动'.format(date)


def dis_actiivty_limit_hero_bi(act_start_short,act_end_short,act_start_time,act_end_time):
    if settings_dev.code == 'superhero_bi':
        for plat in ['superhero_pub', 'superhero_ios']:
            print plat
            dis_actiivty_limit_hero_one(act_start_short, act_end_short, act_start_time, act_end_time, plat)
    else:
        dis_actiivty_limit_hero_one(act_start_short, act_end_short, act_start_time, act_end_time)


def bi_sql(act_start_short, act_end_short, act_start_time, act_end_time, pp):
    # action信息
    active_sql = '''
         select uid, args from raw_action_log where ds>='{act_start_short}' and ds<='{act_end_short}' and act_time>='{act_start_time}' and act_time<='{act_end_time}'
         and action = 'gacha.get_gacha' and substr(uid,1,1) = '{pp}'
    '''.format(act_start_short=act_start_short, act_end_short=act_end_short, act_start_time=act_start_time, act_end_time=act_end_time, pp=pp)
    # 用户信息
    info_sql = '''
           select uid, reverse(substr(reverse(uid),8)) as server, max(vip_level) as vip_level from raw_info where ds >= '{act_start_short}' and ds<='{act_end_short}'
           and substr(uid,1,1) = '{pp}' group by uid, server
       '''.format(act_start_short=act_start_short, act_end_short=act_end_short, pp=pp)
    #消费信息
    spend_sql = '''
        select uid, sum(coin_num) as spend from raw_spendlog where ds >= '{act_start_short}' and ds<='{act_end_short}' and subtime>='{act_start_time}' and subtime<='{act_end_time}'
        and goods_type = 'gacha.get_gacha' and substr(uid,1,1) = '{pp}' group by uid
    '''.format(act_start_short=act_start_short, act_end_short=act_end_short, act_start_time=act_start_time, act_end_time=act_end_time, pp=pp)
    #充值信息
    pay_sql = '''
        select uid, sum(order_money) as money from raw_paylog where ds >= '{act_start_short}' and ds<='{act_end_short}' and substr(uid,1,1) = '{pp}' group by uid
    '''.format(act_start_short=act_start_short, act_end_short=act_end_short, pp=pp)

    return active_sql, info_sql, spend_sql, pay_sql

def dis_actiivty_limit_hero_one(act_start_short,act_end_short,act_start_time,act_end_time,plat=None):

    #action信息
    active_sql = '''
         select uid, args from raw_action_log where ds>='{act_start_short}' and ds<='{act_end_short}' and act_time>='{act_start_time}' and act_time<='{act_end_time}'
         and action = 'gacha.get_gacha'
    '''.format(act_start_short=act_start_short, act_end_short=act_end_short, act_start_time=act_start_time, act_end_time=act_end_time)
    # 用户信息
    info_sql = '''
           select uid, reverse(substr(reverse(uid),8)) as server, max(vip_level) as vip_level from raw_info where ds >= '{act_start_short}' and ds<='{act_end_short}'
           group by uid, server
       '''.format(act_start_short=act_start_short, act_end_short=act_end_short)
    #消费信息
    spend_sql = '''
        select uid, sum(coin_num) as spend from raw_spendlog where ds >= '{act_start_short}' and ds<='{act_end_short}' and subtime>='{act_start_time}' and subtime<='{act_end_time}'
        and goods_type = 'gacha.get_gacha' group by uid
    '''.format(act_start_short=act_start_short, act_end_short=act_end_short, act_start_time=act_start_time, act_end_time=act_end_time)
    #充值信息
    pay_sql = '''
        select uid, sum(order_money) as money from raw_paylog where ds >= '{act_start_short}' and ds<='{act_end_short}' group by uid
    '''.format(act_start_short=act_start_short, act_end_short=act_end_short)

    #根据plat更新sql
    if plat == 'superhero_pub':
        active_sql, info_sql, spend_sql, pay_sql = bi_sql(act_start_short, act_end_short, act_start_time, act_end_time, 'g')
    if plat == 'superhero_ios':
        active_sql, info_sql, spend_sql, pay_sql = bi_sql(act_start_short, act_end_short, act_start_time, act_end_time, 'a')
    if settings_dev.platform == 'superhero_vt':
        pay_sql = '''
            select uid, sum(order_rmb) as money from raw_paylog where ds >= '{act_start_short}' and ds<='{act_end_short}' group by uid
        '''.format(act_start_short=act_start_short, act_end_short=act_end_short)
        # print pay_sql


    #sql转数据
    active_df = hql_to_df(active_sql)
    # print active_df.head(10)
    user_list, sort_list = [], []
    for i in range(len(active_df)):
        tar = eval(active_df.iloc[i, 1])
        # sort_id = 0
        try:
            sort_id = tar['gacha_type']
            sort_list.append(sort_id)
            user_list.append(active_df.iloc[i, 0])
        except:
            pass
    active_df = pd.DataFrame({'uid': user_list, 'gacha_type': sort_list})
    active_df['gacha_type'] = active_df['gacha_type'].astype("str")
    # print active_df.head(10)
    # print active_df.dtypes
    score0 = active_df[active_df['gacha_type'].isin(["['101']", "['102']", "['111']", "['112']", "['999']"])]
    score0['core'] = 0
    score10 = active_df[active_df['gacha_type'].isin(["['201']", "['202']", "['203']", "['204']", "['221']"])]
    score10['core'] = 10
    # print score10.head(10)
    score110 = active_df[active_df['gacha_type'].isin(["['301']", "['302']", "['303']", "['304']", "['305']", "['306']", "['307']", "['321']", "['331']"])]
    score110['core'] = 110
    # print score110.head(10)
    active_df = pd.concat([score0, score10, score110])
    active_df['times'] = 1
    active_df = active_df.groupby('uid').agg({
        'times': lambda g: g.count(),
        'core': lambda g: g.sum()
    }).reset_index()
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
    server_df = result_df.groupby('server').sum().spend.reset_index().rename(columns={'spend': 'server_spend'})
    # print server_df.head(10)
    result_df = result_df.merge(server_df, on='server', how='left')
    result_df = result_df.sort_values(by=['core', 'spend'], ascending=False)[0:500]
    result_df['ds'] = act_start_short
    result_df['rank'] = range(1, (len(result_df) + 1))
    columns = ['ds', 'rank', 'uid', 'server', 'vip_level', 'times', 'core', 'money', 'spend', 'server_spend']
    result_df = result_df[columns]
    # print result_df.head(10)

    # 更新MySQL表
    table = 'dis_activity_limit_hero'
    del_sql = 'delete from {0} where ds="{1}"'.format(table, act_start_short)
    update_mysql(table, result_df, del_sql, plat)
    print act_start_short, table, 'complete'


if __name__ == '__main__':
    # for platform in ('superhero_bi', 'superhero_vt', 'superhero_qiku', 'superhero_tw', 'superhero_self_en'):
        settings_dev.set_env('superhero_tw')
        for date in date_range('20170113', '20170208'):
            dis_actiivty_limit_hero(date)
