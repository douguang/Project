#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Dong Junshuang
Description : 新服 - 活动数据 - 消耗元宝获得灵符
新服：包括14天
'''
import settings_dev
import pandas as pd
import numpy as np
from utils import DateFormat
from utils import get_active_conf
from utils import hql_to_df
from utils import ds_short
from utils import update_mysql
from utils import date_range
from utils import get_config
from utils import get_server_days
from utils import get_server_active_conf


# if __name__ == '__main__':
#     settings_dev.set_env('dancer_mul')
#     date = '20170709'
#     # for date in date_range('20170701', '20170710'):
#     #     print date
#     # act_crontract(date)
def act_crontract(date):
    active_name = 'server_oracle_reward'
    server_days_df = get_server_days(date)
    dfs = []
    for days in range(1, 15):
        version, act_days, act_start_time, act_end_time = get_server_active_conf(
            active_name, date, str(days))
        print version, act_days, act_start_time, act_end_time
        if version == '':
            continue
        else:
            # 选择服务器
            new_list = []
            new_server_df = server_days_df[server_days_df['days'] == days]
            for _, row in new_server_df.iterrows():
                new_list.append(row.server)
            server_lists = str(new_list).replace(
                '[', '(').replace(']', ')').replace('u', '')

            if new_server_df.count().server > 0:
                act_start_short = ds_short(act_start_time)
                act_end_short = ds_short(act_end_time)
                active_sql = '''
                SELECT user_id, server, vip, a_tar, (NVL(money_diff, 0) + NVL(freemoney_diff, 0)) sum_coin
                FROM parse_actionlog
                WHERE ds >= '{act_start_short}'
                  AND ds<='{act_end_short}'
                  AND log_t >='{act_start_time}'
                  AND log_t <= '{act_end_time}'
                  AND a_typ = 'server_oracle_reward.predict'
                  AND reverse(substr(reverse(user_id),8)) IN {server_lists}
                '''.format(act_start_short=act_start_short,
                           act_end_short=act_end_short,
                           act_start_time=act_start_time,
                           act_end_time=act_end_time,
                           server_lists=server_lists)
                # print active_sql
                active_df = hql_to_df(active_sql)
                active_df['sum_coin'] = 0 - active_df['sum_coin']

                # 解析active_df
                def get_active_data():
                    for _, row in active_df.iterrows():
                        yield [row.user_id, row.server, row.vip, row.sum_coin,
                               eval(row.a_tar).get('oracle')]

                # 生成dataframe
                active_result = pd.DataFrame(get_active_data(),
                                             columns=['user_id', 'server',
                                                      'vip', 'spend_coin',
                                                      'oracle'])

                # 取前500名的充值钻石数据
                base_df = (active_result.groupby(
                    ['user_id', 'server']).sum().spend_coin.reset_index()
                           .sort_values(by='spend_coin',
                                        ascending=False)[0:500])
                # 排名
                base_df['rank'] = range(1, (len(base_df) + 1))

                # oracle: 0~2 对应 英雄、装备、资源神域
                oracle_df = (active_result.groupby(
                    ['user_id', 'oracle']).sum().reset_index().pivot_table(
                        'spend_coin', ['user_id'], 'oracle').reset_index()
                             .fillna(0).rename(columns={'0': 'hero_spend',
                                                        '1': 'equip_spend',
                                                        '2': 'source_spend'}))
                # 获取VIP等级
                vip_df = active_result.groupby('user_id').max(
                ).vip.reset_index()

                # 结果
                result_df = (base_df.merge(oracle_df, on='user_id')
                             .merge(vip_df, on='user_id'))
                result_df['ds'] = act_start_short
                columns = ['ds', 'rank', 'user_id', 'server', 'vip',
                           'spend_coin', 'hero_spend', 'equip_spend',
                           'source_spend']
                result_df = result_df[columns]
                dfs.append(result_df)

    if len(dfs) == 0:
        print '无活动数据'
    else:
        oracle_df = pd.concat(dfs)
        # 更新MySQL
        table = 'dis_activity_oracle'
        del_sql = 'delete from {0} where ds="{1}"'.format(table,
                                                          act_start_short)
        update_mysql(table, oracle_df, del_sql)
        print 'dis_activity_oracle complete'


if __name__ == '__main__':
    settings_dev.set_env('dancer_mul')
    for date in date_range('20170710', '20170710'):
        print date
        act_crontract(date)
