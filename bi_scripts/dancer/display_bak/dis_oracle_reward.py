#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Dong Junshuang
Description :
'''
from utils import get_active_conf, hql_to_df, ds_short, update_mysql
import settings_dev
import pandas as pd

def dis_oracle_reward(date):
    version, act_start_time, act_end_time = get_active_conf('oracle_reward',
                                                            date)
    if version != '':
        act_start_short = ds_short(act_start_time)
        act_end_short = ds_short(act_end_time)
        print version, act_start_time, act_end_time
        dis_oracle_reward_one(act_start_short,act_end_short,act_start_time,act_end_time)
    else:
        print '{0} 没有神域活动'.format(date)

def dis_oracle_reward_one(act_start_short,act_end_short,act_start_time,act_end_time):
    active_sql = '''
    SELECT user_id, server, vip, a_tar, (coin_charge+coin_free) sum_coin
    FROM parse_actionlog
    WHERE ds >= '{act_start_short}'
      AND ds<='{act_end_short}'
      AND log_t >='{act_start_time}'
      AND log_t <= '{act_end_time}'
      AND a_typ = 'oracle_reward.predict'
    '''.format(act_start_short=act_start_short,
               act_end_short=act_end_short,
               act_start_time=act_start_time,
               act_end_time=act_end_time)
    # print active_sql
    active_df = hql_to_df(active_sql)

    # 解析active_df
    def get_active_data():
        for _, row in active_df.iterrows():
            yield [row.user_id, row.server, row.vip, row.sum_coin,
                   eval(row.a_tar).get('oracle')]

    # 生成dataframe
    active_result = pd.DataFrame(
        get_active_data(),
        columns=['user_id', 'server', 'vip', 'spend_coin', 'oracle'])

    # 取前500名的充值钻石数据
    base_df = (active_result.groupby(
        ['user_id', 'server']).sum().spend_coin.reset_index()
               .sort_values(by='spend_coin', ascending=False)[0:500])
    # 排名
    base_df['rank'] = range(1, (len(base_df) + 1))

    # oracle: 0~2 对应 英雄、装备、资源神域
    oracle_df = (
        active_result.groupby(['user_id', 'oracle']).sum().reset_index()
        .pivot_table('spend_coin', ['user_id'], 'oracle').reset_index()
        .fillna(0).rename(columns={'0': 'hero_spend',
                                   '1': 'equip_spend',
                                   '2': 'source_spend'}))
    # 获取VIP等级
    vip_df = active_result.groupby('user_id').max().vip.reset_index()

    # 结果
    result_df = (base_df.merge(oracle_df, on='user_id')
                 .merge(vip_df, on='user_id'))
    result_df['ds'] = act_start_short
    columns = ['ds','rank', 'user_id', 'server', 'vip', 'spend_coin', 'hero_spend',
               'equip_spend', 'source_spend']
    result_df = result_df[columns]

    # 更新MySQL表
    table = 'dis_oracle_reward'
    del_sql = 'delete from {0} where ds="{1}"'.format(table, date)
    update_mysql(table, result_df, del_sql)
    print date, table, 'complete'


if __name__ == '__main__':
    settings_dev.set_env('dancer_tw')
    date = '20161223'
    dis_oracle_reward(date)
