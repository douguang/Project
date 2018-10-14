#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Dong Junshuang
Description : 合服数据
Time        : 2017.06.15
illustration: 将Excel中的合服数据整理为，主服  从服，...，从服
'''
import settings_dev
import pandas as pd
from utils import ds_add
from utils import get_rank
from utils import hql_to_df
from dancer.cfg import zichong_uids

if __name__ == '__main__':
    settings_dev.set_env('dancer_tw')
    date = '20170614'

    # 获取合服数据
    hefu_df = pd.read_excel(
        r'/Users/kaiqigu/Documents/Excel/dancer_server.xlsx')

    def get_server_list():
        for _, row in hefu_df.iterrows():
            for son_server in row.son_server.split(','):
                yield [row.server, son_server]

    # 生成子服的DataFrame
    server_list_df = pd.DataFrame(get_server_list(),
                                  columns=['server', 'son_server'])

    server_sql = '''
    SELECT t1.ds,
           t1.user_id,
           t1.server as son_server,
           combat,
           nvl(t2.order_money, 0) AS sum_money,
           CASE WHEN t2.order_money > 0 THEN 1 ELSE 0 end AS is_pay
    FROM
      (SELECT user_id,
              reverse(substr(reverse(user_id), 8)) AS server,
              ds,
              combat
       FROM parse_info
       WHERE ds>='{date_ago}'
         AND ds<='{date}') t1
    LEFT JOIN
      (SELECT user_id,
              sum(order_money) AS order_money,
              ds
       FROM raw_paylog
       WHERE ds>='{date_ago}'
         AND ds<='{date}'
         AND platform_2<>'admin_test'
         AND order_id NOT LIKE '%testktwwn%'
       GROUP BY user_id,
                ds) t2 ON t1.ds=t2.ds
    AND t1.user_id = t2.user_id
    '''.format(date=date, date_ago=ds_add(date, -6))
    server_df = hql_to_df(server_sql)

    # 排除测试用户
    server_df = server_df[~server_df['user_id'].isin(zichong_uids)]

    server_result = server_df.merge(server_list_df,
                                    on='son_server',
                                    how='left').fillna('None')
    # 合服的数据 - 服务器、DAU、充值人数、充值总额
    server_result_df = server_result[server_result.server != 'None']
    column = ['server', 'dau', 'pay_num', 'sum_money']
    server_result_three = server_result_df.groupby('server').user_id.count(
    ).reset_index().rename(columns={'user_id': 'dau'})
    server_result_three['dau'] = server_result_three['dau'] * 1.0 / 7
    # 对user_id去重、汇总
    server_result_one = server_result_df.groupby(['user_id', 'server']).agg({
        'combat': 'max',
        'sum_money': 'sum',
        'is_pay': 'max',
    }).reset_index()
    # 统计每个服的汇总数据
    server_result_two = server_result_one.groupby('server').agg({
        'sum_money': 'sum',
        'is_pay': 'sum',
    }).reset_index().rename(columns={'is_pay': 'pay_num'})
    server_result_two = server_result_two.merge(server_result_three,
                                                on='server',
                                                how='left')[column]

    # 获取合服数据的每个服的前10名
    dfs = []
    for server in set(server_result_one.server.tolist()):
        df = server_result_one[server_result_one.server == server]
        df = get_rank(df, 'combat', 10)
        dfs.append(df)
    server_zhu_result = pd.concat(dfs)
    column = ['user_id', 'server', 'sum_money', 'is_pay', 'combat', 'rank']
    server_zhu_result = server_zhu_result[column]

    # 未合服的数据 - 服务器、DAU、充值人数、充值总额
    no_server_df = server_result[server_result.server == 'None']
    column = ['son_server', 'dau', 'pay_num', 'sum_money']
    no_server_result_three = no_server_df.groupby('son_server').user_id.count(
    ).reset_index().rename(columns={'user_id': 'dau'})
    no_server_result_three['dau'] = no_server_result_three['dau'] * 1.0 / 7
    # 对user_id去重、汇总
    no_server_result_one = no_server_df.groupby(['user_id', 'son_server']).agg(
        {
            'combat': 'max',
            'sum_money': 'sum',
            'is_pay': 'max',
        }).reset_index()
    # 统计每个服的汇总数据
    no_server_result_two = no_server_result_one.groupby('son_server').agg({
        'sum_money': 'sum',
        'is_pay': 'sum',
    }).reset_index().rename(columns={'is_pay': 'pay_num'})
    no_server_result_two = no_server_result_two.merge(no_server_result_three,
                                                      on='son_server',
                                                      how='left')[column]
    # 获取未合服数据的每个服的前10名 - user_id、服务器、充值金额、是否充值、战斗力、排名
    dfs = []
    for son_server in set(no_server_result_one.son_server.tolist()):
        df = no_server_result_one[no_server_result_one.son_server ==
                                  son_server]
        df = get_rank(df, 'combat', 10)
        dfs.append(df)
    no_server_result = pd.concat(dfs)
    column = ['user_id', 'son_server', 'sum_money', 'is_pay', 'combat', 'rank']
    no_server_result = no_server_result[column]

    # 导出合服数据
    server_result_two.to_excel(
        '/Users/kaiqigu/Documents/Excel/hefu_he_huizong.xlsx')
    server_zhu_result.to_excel(
        '/Users/kaiqigu/Documents/Excel/hefu_he_combat.xlsx')
    # 导出未合服数据
    no_server_result_two.to_excel(
        '/Users/kaiqigu/Documents/Excel/hefu_wei_huizong.xlsx')
    no_server_result.to_excel(
        '/Users/kaiqigu/Documents/Excel/hefu_wei_combat.xlsx')
