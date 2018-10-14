#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Zhang Yongchen
Description : 消耗元宝获得灵符及随机道具奖励（灵符分三种），灵符可以用来兑换稀有道具，每消耗1元宝获得1积分，达到固定积分获得相应奖励；
每天产生最终大奖的灵符号；所消耗的元宝存入北冥之灵，玩家有机会获得其中的元宝。
contract关键字，购买灵符为magic_school.open_contract内含contract_id字段(1为龙符，2为天符，3为风雷符)，积分兑换物品为magic_school.contract_exchange内含which字段；开服版活动在普通活动前加server_。
Database    : dancer
'''
import settings_dev
from utils import hql_to_df, update_mysql, date_range, ds_short, get_active_conf
import pandas as pd
import numpy as np

def dis_activity_contract(date):

    version, act_start_time, act_end_time = get_active_conf('contract', date)

    if version != '':
        act_start_short = ds_short(act_start_time)
        act_end_short = ds_short(act_end_time)
        print version, act_start_time, act_end_time
        dis_activity_contract_one(act_start_short,act_end_short,act_start_time,act_end_time)
    else:
        print '{0} 没有北冥之灵活动'.format(date)

def dis_activity_contract_one(act_start_short,act_end_short,act_start_time,act_end_time):

    contract_sql = '''
    SELECT user_id,
           (NVL(freemoney_diff,0) + NVL(money_diff,0)) AS spend,
           server,
           a_tar
    FROM parse_actionlog
    WHERE ds>='{act_start_short}'
      AND ds<='{act_end_short}'
      AND log_t>='{act_start_time}'
      AND log_t<='{act_end_time}'
      AND a_typ = 'magic_school.open_contract'
    '''.format(act_start_short=act_start_short, act_end_short=act_end_short, act_start_time=act_start_time, act_end_time=act_end_time)
    # print contract_sql
    oracle_df = hql_to_df(contract_sql)
    oracle_df['ds'] = act_start_short
    # print oracle_df.head(10)

    info_sql = '''
    SELECT user_id,
           vip
    FROM parse_info
    WHERE ds='{date}'
    '''.format(date=act_start_short)
    info_df = hql_to_df(info_sql)
    # print info_df.head(10)

    pay_sql = '''
    SELECT user_id,
           sum(order_money) AS money
    FROM raw_paylog
    WHERE ds='{date}'
      AND platform_2<>'admin_test'
      AND order_id NOT LIKE '%test%'
    GROUP BY user_id
    '''.format(date=act_start_short)
    pay_df = hql_to_df(pay_sql)
    # print pay_df.head(10)

    user_id_list, spend_list, server_list, contract_id_list, ds_list = [], [], [], [], []
    for i in range(len(oracle_df)):
        user_id_list.append(oracle_df.iloc[i, 0])
        spend_list.append(oracle_df.iloc[i, 1])
        server_list.append(oracle_df.iloc[i, 2])
        ds_list.append(oracle_df.iloc[i, 4])
        tar = oracle_df.iloc[i, 3]
        tar = eval(tar)
        contract_id = 0
        try:
            contract_id = tar['contract_id']
        except:
            pass
        contract_id_list.append(contract_id)

    data = pd.DataFrame({'user_id': user_id_list, 'spend': spend_list, 'server': server_list,
                         'contract_id': contract_id_list, 'ds': ds_list})
    data = data[data['contract_id'] != 0]
    data['num'] = 1
    # print data.head(10)
    pivot_df = pd.pivot_table(data, values='num', index='user_id', columns='contract_id', aggfunc=np.sum, fill_value=0).reset_index()
    # print pivot_df.head(10)
    pivot_df.columns = ['user_id', 'longfu', 'tianfu', 'fengleifu']
    # pivot_df.columns = ['user_id'] + ['contract_id%d' %for d in range(3)]
    pivot_df = pivot_df.head(500)
    # print pivot_df.head(10)

    data = data.drop('contract_id', axis=1)
    data = data.groupby(['user_id', 'server', 'ds']).sum().spend.reset_index()
    server_df = data.groupby('server').sum().spend.reset_index().rename(columns={'spend':'server_spend'})
    # print server_df.head(10)
    result_df = pivot_df.merge(data, on='user_id', how='left').merge(server_df, on='server', how='left')
    columns = ['ds', 'user_id', 'server', 'spend', 'longfu', 'tianfu', 'fengleifu', 'server_spend']
    result_df = result_df[columns]
    result_df['spend'] = 0 - result_df['spend']
    result_df['server_spend'] = 0 - result_df['server_spend']
    # print result_df.head(10)
    result_df = result_df.sort_values(by=['spend'], ascending=False)
    # print result_df.head(10)
    result_df['rank'] = range(1, (len(result_df) + 1))
    # print result_df.head(10)
    # result_df.to_excel('/home/kaiqigu/Documents/ceui.xlsx')
    result_df = result_df.merge(info_df, on='user_id', how='left')
    result_df = result_df.merge(pay_df, on='user_id', how='left').fillna(0)
    columns = ['ds', 'user_id', 'server', 'vip', 'money',  'spend', 'server_spend', 'longfu', 'tianfu', 'fengleifu', 'rank']
    result_df = result_df[columns]

    #更新MySQL
    table = 'dis_activity_contract'
    print act_start_short, table
    del_sql = 'delete from {0} where ds="{1}"'.format(table, act_start_short)
    update_mysql(table, result_df, del_sql)

#执行
if __name__ == '__main__':
    # settings_dev.set_env('dancer_tw')
    # dis_activity_contract('20170217')
    for platform in ('dancer_tw', 'dancer_pub'):
        settings_dev.set_env(platform)
        for date in date_range('20170205', '20170220'):
            dis_activity_contract(date)
