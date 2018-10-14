#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Zhang Yongchen
Description : 宇宙最强活动展示页面,取全服玩家的id vip 服务器 战斗力 活动消费 当日充值，按照消费降序排名，计算服务器消费总量。
Database    : dancer
'''
import settings_dev
from utils import hql_to_df, update_mysql, date_range, get_active_conf, ds_short


def dis_activity_super_active(date):

    version, act_start_time, act_end_time = get_active_conf('super_rich', date)

    if version != '':
        act_start_short = ds_short(act_start_time)
        act_end_short = ds_short(act_end_time)
        print version, act_start_time, act_end_time
        dis_activity_super_active_one(act_start_short, act_end_short,
                                      act_start_time, act_end_time)
    else:
        print '{0} 没有宇宙最强活动'.format(date)


def dis_activity_super_active_one(act_start_short, act_end_short,
                                  act_start_time, act_end_time):

    # 用户信息
    info_sql = '''
    SELECT ds,
           user_id,
           reverse(substr(reverse(user_id),8)) AS server,
           vip
    FROM parse_info
    WHERE ds >= '{act_start_short}'
      AND ds<='{act_end_short}'
    '''.format(act_start_short=act_start_short,
               act_end_short=act_end_short)
    # print info_sql
    info_df = hql_to_df(info_sql)
    # print info_df.head(10)

    # 消费信息
    spend_sql = '''
    SELECT user_id,
           sum(coin_num) AS spend
    FROM raw_spendlog
    WHERE ds >= '{act_start_short}'
      AND ds<='{act_end_short}'
      AND subtime>='{act_start_time}'
      AND subtime<='{act_end_time}'
    GROUP BY user_id
    ORDER BY spend DESC LIMIT 500
    '''.format(act_start_short=act_start_short,
               act_end_short=act_end_short,
               act_start_time=act_start_time,
               act_end_time=act_end_time)
    # print spend_sql
    spend_df = hql_to_df(spend_sql)
    # print spend_df.head(10)

    # 充值信息
    pay_sql = '''
    SELECT user_id,
           sum(order_money) AS money
    FROM raw_paylog
    WHERE ds >= '{act_start_short}'
      AND ds<='{act_end_short}'
      AND platform_2<>'admin_test'
      AND order_id NOT LIKE '%test%'
    GROUP BY user_id
    '''.format(act_start_short=act_start_short,
               act_end_short=act_end_short)
    # print pay_sql
    pay_df = hql_to_df(pay_sql)
    # print pay_df.head(10)

    # 排序过程
    sort_df = spend_df.merge(info_df, on='user_id', how='left').fillna(0)
    # print sort_df.head(10)
    sort_df = sort_df.merge(pay_df, on='user_id', how='left').fillna(0)
    # print sort_df.head(10)

    # 全服信息
    server_df = sort_df.groupby('server').sum().spend.reset_index().rename(
        columns={'spend': 'server_spend'})
    # print server_df.head(10)

    # 结果
    result_df = sort_df.merge(server_df, on='server', how='left')
    columns = ['ds', 'user_id', 'server', 'vip', 'money', 'spend',
               'server_spend']
    result_df = result_df[columns]
    result_df['rank'] = result_df['spend'].rank(ascending=False).astype("int")
    # print result_df.head(10)
    # result_df.to_excel('/home/kaiqigu/Documents/superactive.xlsx')

    # 更新MySQL
    table = 'dis_activity_super_active'
    print act_start_short, table
    del_sql = 'delete from {0} where ds="{1}"'.format(table, act_start_short)
    update_mysql(table, result_df, del_sql)


# 执行
if __name__ == '__main__':
    for platform in ('dancer_pub', 'dancer_tw'):
        settings_dev.set_env(platform)
        for date in date_range('20170125', '20170205'):
            dis_activity_super_active(date)
