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
#     # for date in date_range('20170703', '20170710'):
#     #     print date
#     date = '20170703'
#     # act_crontract(date)
def act_crontract(date):
    active_name = 'server_contract'
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
                  AND a_typ = 'server_magic_school.open_contract'
                  AND reverse(substr(reverse(user_id),8)) IN {server_lists}
                '''.format(act_start_short=act_start_short,
                           act_end_short=act_end_short,
                           act_start_time=act_start_time,
                           act_end_time=act_end_time,
                           server_lists=server_lists)
                # print contract_sql
                oracle_df = hql_to_df(contract_sql)
                oracle_df['ds'] = act_start_short
                # print oracle_df.head(10)

                info_sql = '''
                SELECT user_id,
                       vip
                FROM parse_info
                WHERE ds='{date}'
                AND reverse(substr(reverse(user_id),8)) IN {server_lists}
                '''.format(date=act_start_short,
                           server_lists=server_lists)
                info_df = hql_to_df(info_sql)
                # print info_df.head(10)

                pay_sql = '''
                SELECT user_id,
                       sum(order_money) AS money
                FROM raw_paylog
                WHERE ds='{date}'
                  AND platform_2<>'admin_test'
                  AND order_id NOT LIKE '%test%'
                  AND reverse(substr(reverse(user_id),8)) IN {server_lists}
                GROUP BY user_id
                '''.format(date=act_start_short,
                           server_lists=server_lists)
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

                data = pd.DataFrame({'user_id': user_id_list,
                                     'spend': spend_list,
                                     'server': server_list,
                                     'contract_id': contract_id_list,
                                     'ds': ds_list})
                data = data[data['contract_id'] != 0]
                data['num'] = 1
                # print data.head(10)
                pivot_df = pd.pivot_table(data,
                                          values='num',
                                          index='user_id',
                                          columns='contract_id',
                                          aggfunc=np.sum,
                                          fill_value=0).reset_index()
                # print pivot_df.head(10)
                pivot_df.columns = ['user_id', 'longfu', 'tianfu', 'fengleifu']
                # pivot_df.columns = ['user_id'] + ['contract_id%d' %for d in range(3)]
                pivot_df = pivot_df.head(500)
                # print pivot_df.head(10)

                data = data.drop('contract_id', axis=1)
                data = data.groupby(
                    ['user_id', 'server', 'ds']).sum().spend.reset_index()
                server_df = data.groupby('server').sum().spend.reset_index(
                ).rename(columns={'spend': 'server_spend'})
                # print server_df.head(10)
                result_df = pivot_df.merge(data, on='user_id',
                                           how='left').merge(server_df,
                                                             on='server',
                                                             how='left')
                columns = ['ds', 'user_id', 'server', 'spend', 'longfu',
                           'tianfu', 'fengleifu', 'server_spend']
                result_df = result_df[columns]
                result_df['spend'] = 0 - result_df['spend']
                result_df['server_spend'] = 0 - result_df['server_spend']
                # print result_df.head(10)
                result_df = result_df.sort_values(by=['spend'],
                                                  ascending=False)
                # print result_df.head(10)
                result_df['rank'] = range(1, (len(result_df) + 1))
                # print result_df.head(10)
                # result_df.to_excel('/home/kaiqigu/Documents/ceui.xlsx')
                result_df = result_df.merge(info_df, on='user_id', how='left')
                result_df = result_df.merge(pay_df,
                                            on='user_id',
                                            how='left').fillna(0)
                columns = ['ds', 'user_id', 'server', 'vip', 'money', 'spend',
                           'server_spend', 'longfu', 'tianfu', 'fengleifu',
                           'rank']
                result_df = result_df[columns]
                dfs.append(result_df)

    if len(dfs) == 0:
        print '无活动数据'
    else:
        contract_df = pd.concat(dfs)
        # 更新MySQL
        table = 'dis_activity_contract'
        print act_start_short, table
        del_sql = 'delete from {0} where ds="{1}"'.format(table,
                                                          act_start_short)
        update_mysql(table, contract_df, del_sql)
        print 'dis_activity_contract complete'


if __name__ == '__main__':
    settings_dev.set_env('dancer_mul')
    for date in date_range('20170701', '20170710'):
        print date
        act_crontract(date)
