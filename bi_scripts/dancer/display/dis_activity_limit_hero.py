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

def dis_activity_limit_hero(date):

    version, act_start_time, act_end_time = get_active_conf('hero_version', date)

    if version != '':
        act_start_short = ds_short(act_start_time)
        act_end_short = ds_short(act_end_time)
        print version, act_start_time, act_end_time
        dis_activity_limit_hero_one(act_start_short,act_end_short,act_start_time,act_end_time)
    else:
        print '{0} 没有限时神将活动'.format(date)

def dis_activity_limit_hero_one(act_start_short,act_end_short,act_start_time,act_end_time):

    #action信息
    active_sql = '''
    SELECT user_id,
           a_tar
    FROM parse_actionlog
    WHERE ds>='{act_start_short}'
      AND ds<='{act_end_short}'
      AND log_t>='{act_start_time}'
      AND log_t<='{act_end_time}'
      AND a_typ = 'gacha.get_gacha'
    '''.format(act_start_short=act_start_short, act_end_short=act_end_short, act_start_time=act_start_time, act_end_time=act_end_time)
    active_df = hql_to_df(active_sql)
    # print active_df.head(10)

    user_list, sort_list = [], []
    for i in range(len(active_df)):

        tar = eval(active_df.iloc[i, 1])
        # sort_id = 0
        try:
            mobage_id = tar['mobage_id']
            sort_id = tar['gacha_sort']
            sort_list.append(sort_id)
            user_list.append(active_df.iloc[i, 0])
        except:
            pass

    active_df = pd.DataFrame({'user_id': user_list, 'gacha_sort': sort_list})
    active_df['gacha_sort'] = active_df['gacha_sort'].astype("int")
    # print active_df.head(10)
    # print active_df.dtypes
    score0 = active_df[active_df['gacha_sort'].isin([1, 4])]
    score0['core'] = 0
    score10 = active_df[active_df['gacha_sort'] == 2]
    score10['core'] = 10
    # print score10.head(10)
    score110 = active_df[active_df['gacha_sort'].isin([3, 5, 6, 7, 8])]
    score110['core'] = 110
    # print score110.head(10)
    active_df = pd.concat([score0, score10, score110])
    active_df['times'] = 1
    active_df = active_df.groupby('user_id').agg({
        'times': lambda g: g.count(),
        'core': lambda g: g.sum()
    }).reset_index()
    # print active_df.head(10)

    # 用户信息
    info_sql = '''
    SELECT user_id,
           reverse(substr(reverse(user_id),8)) AS server,
           max(vip) AS vip
    FROM parse_info
    WHERE ds >= '{act_start_short}'
      AND ds<='{act_end_short}'
    GROUP BY user_id,
             server
    '''.format(act_start_short=act_start_short, act_end_short=act_end_short)
    # print info_sql
    info_df = hql_to_df(info_sql)
    # print info_df.head(10)

    #消费信息
    spend_sql = '''
    SELECT user_id,
           sum(coin_num) AS spend
    FROM raw_spendlog
    WHERE ds >= '{act_start_short}'
      AND ds<='{act_end_short}'
      AND subtime>='{act_start_time}'
      AND subtime<='{act_end_time}'
      AND goods_type = 'gacha.get_gacha'
    GROUP BY user_id
    '''.format(act_start_short=act_start_short, act_end_short=act_end_short, act_start_time=act_start_time, act_end_time=act_end_time)
    # print spend_sql
    spend_df = hql_to_df(spend_sql)
    # print spend_df.head(10)

    #充值信息
    pay_sql = '''
    SELECT user_id,
           sum(order_money) AS money
    FROM raw_paylog
    WHERE ds >= '{act_start_short}'
      AND ds<='{act_end_short}'
      AND platform_2<>'admin_test'
      AND order_id NOT LIKE '%test%'
    GROUP BY user_id
    '''.format(act_start_short=act_start_short, act_end_short=act_end_short)
    # print pay_sql
    pay_df = hql_to_df(pay_sql)
    # print pay_df.head(10)

    #合并数据
    result_df = active_df.merge(info_df, on='user_id', how='left').fillna(0)
    # print result_df.dtypes
    result_df = result_df.merge(spend_df, on='user_id', how='left').fillna(0)
    # print result_df.dtypes
    result_df = result_df.merge(pay_df, on='user_id', how='left').fillna(0)
    # print result_df.head(10)
    #全服信息
    server_df = result_df.groupby('server').sum().spend.reset_index().rename(columns={'spend': 'server_spend'})
    # print server_df.head(10)
    result_df = result_df.merge(server_df, on='server', how='left')
    result_df = result_df.sort_values(by=['core', 'spend'], ascending=False)[0:500]
    result_df['ds'] = act_start_short
    result_df['rank'] = range(1, (len(result_df) + 1))
    columns = ['ds', 'rank', 'user_id', 'server', 'vip', 'times', 'core', 'money', 'spend', 'server_spend']
    result_df = result_df[columns]

    # print result_df.head(10)

    # 更新MySQL表
    table = 'dis_activity_limit_hero'
    del_sql = 'delete from {0} where ds="{1}"'.format(table, act_start_short)
    update_mysql(table, result_df, del_sql)
    print act_start_short, table, 'complete'


if __name__ == '__main__':
    for platform in ('dancer_pub', 'dancer_tw'):
        settings_dev.set_env(platform)
        for date in date_range('20170125', '20170205'):
            dis_activity_limit_hero(date)
