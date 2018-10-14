#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Zhang Yongchen
Description : 宇宙最强活动展示页面,取全服玩家的id vip_level 服务器 战斗力 活动消费 当日充值，按照消费降序排名，计算服务器消费总量。(已完成)
Database    : dancer
'''
import settings_dev
from utils import hql_to_df, update_mysql, date_range, get_active_conf, ds_short

def dis_activity_super_active(date):

    version, act_start_time, act_end_time = get_active_conf('super_rich', date)

    print version, act_start_time, act_end_time

    if version != '':
        act_start_short = ds_short(act_start_time)
        act_end_short = ds_short(act_end_time)
        print version, act_start_time, act_end_time
        dis_activity_super_active_bi(act_start_short,act_end_short,act_start_time,act_end_time)
    else:
        print '{0} 没有宇宙最强活动'.format(date)

def dis_activity_super_active_bi(act_start_short,act_end_short,act_start_time,act_end_time):
    if settings_dev.code == 'superhero_bi':
        for plat in ['superhero_pub', 'superhero_ios']:
            print plat
            dis_activity_super_active_one(act_start_short, act_end_short, act_start_time, act_end_time, plat)
    else:
        dis_activity_super_active_one(act_start_short, act_end_short, act_start_time, act_end_time)

def bi_sql(act_start_short, act_end_short, act_start_time, act_end_time, pp):

    #用户信息
    info_sql = '''
        select ds, uid, reverse(substr(reverse(uid),8)) as server, vip_level from raw_info where ds >= '{act_start_short}' and ds<='{act_end_short}' and substr(uid,1,1) = '{pp}'
    '''.format(act_start_short=act_start_short, act_end_short=act_end_short, pp=pp)
    # print info_sql

    #消费信息
    spend_sql = '''
        select uid, sum(coin_num) as spend from raw_spendlog where ds >= '{act_start_short}' and ds<='{act_end_short}' and substr(uid,1,1) = '{pp}' group by uid order by spend desc limit 500
    '''.format(act_start_short=act_start_short, act_end_short=act_end_short, act_start_time=act_start_time, act_end_time=act_end_time, pp=pp)
    # print spend_sql

    #充值信息
    pay_sql = '''
        select uid, sum(order_money) as money from raw_paylog where ds >= '{act_start_short}' and ds<='{act_end_short}' and substr(uid,1,1) = '{pp}' group by uid
    '''.format(act_start_short=act_start_short, act_end_short=act_end_short, pp=pp)

    return info_sql, spend_sql, pay_sql

def dis_activity_super_active_one(act_start_short, act_end_short, act_start_time, act_end_time, plat=None):

    # plat = plat or settings_dev.platform

    #用户信息
    info_sql = '''
        select ds, uid, reverse(substr(reverse(uid),8)) as server, vip_level from raw_info where ds >= '{act_start_short}' and ds<='{act_end_short}'
    '''.format(act_start_short=act_start_short, act_end_short=act_end_short)
    # print info_sql

    #消费信息
    spend_sql = '''
        select uid, sum(coin_num) as spend from raw_spendlog where ds >= '{act_start_short}' and ds<='{act_end_short}' group by uid order by spend desc limit 500
    '''.format(act_start_short=act_start_short, act_end_short=act_end_short, act_start_time=act_start_time, act_end_time=act_end_time)
    # print spend_sql

    #充值信息
    pay_sql = '''
        select uid, sum(order_money) as money from raw_paylog where ds >= '{act_start_short}' and ds<='{act_end_short}' group by uid
    '''.format(act_start_short=act_start_short, act_end_short=act_end_short)
    # print pay_sql

    if plat == 'superhero_pub':
        info_sql, spend_sql, pay_sql = bi_sql(act_start_short, act_end_short, act_start_time, act_end_time, 'g')
    if plat == 'superhero_ios':
        info_sql, spend_sql, pay_sql = bi_sql(act_start_short, act_end_short, act_start_time, act_end_time, 'a')
    if settings_dev.platform == 'superhero_vt':
        pay_sql = '''
            select uid, sum(order_rmb) as money from raw_paylog where ds >= '{act_start_short}' and ds<='{act_end_short}' group by uid
        '''.format(act_start_short=act_start_short, act_end_short=act_end_short)
        # print pay_sql

    info_df = hql_to_df(info_sql)
    # print info_df.head(10)
    spend_df = hql_to_df(spend_sql)
    # print spend_df.head(10)
    pay_df = hql_to_df(pay_sql)
    # print pay_df.head(10)

    #排序过程
    sort_df = spend_df.merge(info_df, on='uid', how='left').fillna(0)
    # print sort_df.head(10)
    sort_df = sort_df.merge(pay_df, on='uid', how='left').fillna(0)
    # print sort_df.head(10)

    #全服信息
    server_df = sort_df.groupby('server').sum().spend.reset_index().rename(columns={'spend':'server_spend'})
    # print server_df.head(10)

    #结果
    result_df = sort_df.merge(server_df, on='server', how='left')
    columns = ['ds', 'uid', 'server', 'vip_level', 'money',  'spend', 'server_spend']
    result_df = result_df[columns]
    result_df['rank'] = result_df['spend'].rank(ascending=False).astype("int")
    # print result_df.head(10)
    # result_df.to_excel('/home/kaiqigu/Documents/superactive.xlsx')


    #更新MySQL
    table = 'dis_activity_super_active'
    print act_start_short,table
    del_sql = 'delete from {0} where ds="{1}"'.format(table, act_start_short)
    update_mysql(table, result_df, del_sql, plat)

#执行
if __name__ == '__main__':
    for platform in ('superhero_bi', 'superhero_vt', 'superhero_qiku', 'superhero_tw', 'superhero_self_en'):
        settings_dev.set_env(platform)
        for date in date_range('20170101', '20170105'):
            dis_activity_super_active(date)
