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
#     date = '20170703'
#     # for date in date_range('20170701', '20170710'):
#     #     print date
#     # act_crontract(date)
def act_crontract(date):
    active_name = 'server_roulette'
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
                # action信息
                active_sql_10 = '''
                SELECT user_id,
                       a_typ
                FROM parse_actionlog
                WHERE ds>='{act_start_short}'
                  AND ds<='{act_end_short}'
                  AND log_t>='{act_start_time}'
                  AND log_t<='{act_end_time}'
                  AND a_typ = 'server_roulette.open_roulette10'
                  AND reverse(substr(reverse(user_id),8)) IN {server_lists}
                    '''.format(act_start_short=act_start_short,
                               act_end_short=act_end_short,
                               act_start_time=act_start_time,
                               act_end_time=act_end_time,
                               server_lists=server_lists)
                # print active_sql_10
                active_df_10 = hql_to_df(active_sql_10)
                active_df_10['core'] = 110
                active_sql_1 = '''
                SELECT user_id,
                       a_typ
                FROM parse_actionlog
                WHERE ds>='{act_start_short}'
                  AND ds<='{act_end_short}'
                  AND log_t>='{act_start_time}'
                  AND log_t<='{act_end_time}'
                  AND a_typ = 'server_roulette.open_roulette'
                  AND reverse(substr(reverse(user_id),8)) IN {server_lists}
                    '''.format(act_start_short=act_start_short,
                               act_end_short=act_end_short,
                               act_start_time=act_start_time,
                               act_end_time=act_end_time,
                               server_lists=server_lists)
                active_df_1 = hql_to_df(active_sql_1)
                active_df_1['core'] = 10
                active_df = pd.concat([active_df_1, active_df_10])
                active_df = active_df.groupby('user_id').agg({
                    'a_typ': lambda g: g.count(),
                    'core': lambda g: g.sum()
                }).rename(columns={'a_typ': 'times'}).reset_index()
                # active_df['ds'] = act_start_short
                # active_df['user_id'] = active_df['user_id'].astype("str")
                # print active_df.head(10)

                # 用户信息
                info_sql = '''
                SELECT user_id,
                       reverse(substr(reverse(user_id),8)) AS server,
                       max(vip) AS vip
                FROM parse_info
                WHERE ds >= '{act_start_short}'
                  AND ds<='{act_end_short}'
                  AND reverse(substr(reverse(user_id),8)) IN {server_lists}
                GROUP BY user_id,
                         server
                   '''.format(act_start_short=act_start_short,
                              act_end_short=act_end_short,
                              server_lists=server_lists)
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
                  AND goods_type LIKE '%roulette%'
                  AND reverse(substr(reverse(user_id),8)) IN {server_lists}
                GROUP BY user_id
                '''.format(act_start_short=act_start_short,
                           act_end_short=act_end_short,
                           act_start_time=act_start_time,
                           act_end_time=act_end_time,
                           server_lists=server_lists)
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
                  AND reverse(substr(reverse(user_id),8)) IN {server_lists}
                GROUP BY user_id
                '''.format(act_start_short=act_start_short,
                           act_end_short=act_end_short,
                           server_lists=server_lists)
                # print pay_sql
                pay_df = hql_to_df(pay_sql)
                # print pay_df.head(10)

                # 合并数据
                result_df = active_df.merge(info_df,
                                            on='user_id',
                                            how='left').fillna(0)
                # print result_df.dtypes
                result_df = result_df.merge(spend_df,
                                            on='user_id',
                                            how='left').fillna(0)
                # print result_df.dtypes
                result_df = result_df.merge(pay_df,
                                            on='user_id',
                                            how='left').fillna(0)
                # print result_df.head(10)
                # 全服信息
                server_df = result_df.groupby('server').sum(
                ).spend.reset_index().rename(columns={'spend': 'server_spend'})
                # print server_df.head(10)
                result_df = result_df.merge(server_df, on='server', how='left')
                result_df = result_df.sort_values(by=['core', 'spend'],
                                                  ascending=False)[0:500]
                result_df['ds'] = act_start_short
                result_df['rank'] = range(1, (len(result_df) + 1))
                columns = ['ds', 'rank', 'user_id', 'server', 'vip', 'times',
                           'core', 'money', 'spend', 'server_spend']
                result_df = result_df[columns]
                dfs.append(result_df)

    if len(dfs) == 0:
        print '无活动数据'
    else:
        roulette_df = pd.concat(dfs)
        # 更新MySQL
        table = 'dis_activity_roulette'
        del_sql = 'delete from {0} where ds="{1}"'.format(table,
                                                          act_start_short)
        update_mysql(table, roulette_df, del_sql)
        print 'dis_activity_roulette complete'


if __name__ == '__main__':
    settings_dev.set_env('dancer_mul')
    for date in date_range('20170704', '20170710'):
        print date
        act_crontract(date)
