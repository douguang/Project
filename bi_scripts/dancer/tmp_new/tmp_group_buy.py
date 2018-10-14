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
from pandas import DataFrame
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
    active_name = 'server_group_version'
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
                sql = '''
                    select user_id,reverse(substr(reverse(user_id), 8)) as server,a_tar
                    from parse_actionlog
                    WHERE ds >= '{act_start_short}'
                    AND ds<='{act_end_short}'
                    AND log_t >='{act_start_time}'
                    AND log_t <= '{act_end_time}'
                    and a_typ like  '%group_active_buy%'
                    AND reverse(substr(reverse(user_id),8)) IN {server_lists}
                '''.format(act_start_short=act_start_short,
                           act_end_short=act_end_short,
                           act_start_time=act_start_time,
                           act_end_time=act_end_time,
                           server_lists=server_lists)
                df = hql_to_df(sql)
                df = df.fillna(0)
                df['ds'] = act_start_short
                # print df.head(10)

                if df.__len__() != 0:
                    ds_list, user_id_list, server_list, item_id_list, count_list = [], [], [], [], []
                    for i in range(len(df)):
                        ds = df.iloc[i, 3]
                        user_id = df.iloc[i, 0]
                        server = df.iloc[i, 1]
                        tar = df.iloc[i, 2]
                        tar = eval(tar)
                        item_id = tar['shop_id']
                        count = int(tar['count'])
                        ds_list.append(ds)
                        user_id_list.append(user_id)
                        server_list.append(server)
                        item_id_list.append(item_id)
                        count_list.append(count)

                    data = DataFrame({'ds': ds_list,
                                      'server': server_list,
                                      'item_id': item_id_list,
                                      'count': count_list,
                                      'user_id': user_id_list})
                    buy_num = data.groupby(['ds', 'server', 'item_id']).agg(
                        {'user_id': lambda g: g.nunique()}).reset_index()
                    buy_num = buy_num.rename(
                        columns={'user_id': 'buy_user_num', })
                    count_num = data.groupby(['ds', 'server', 'item_id']).agg(
                        {'count': lambda g: g.sum()}).reset_index()
                    count_num = count_num.rename(
                        columns={'user_id': 'buy_num', })

                    result = DataFrame(buy_num).merge(
                        count_num,
                        on=['ds', 'server', 'item_id'],
                        how='left')
                    result_df = DataFrame(result).fillna(0)
                    # print result_df.head(5)

                    result_df['ds'] = result_df['ds'].astype("str")
                    result_df['server'] = result_df['server'].astype("str")
                    result_df['item_id'] = result_df['item_id'].astype("int")
                    result_df['buy_user_num'] = result_df[
                        'buy_user_num'].astype("int")
                    result_df['count'] = result_df['count'].astype("int")
                dfs.append(result_df)

    if len(dfs) == 0:
        print '无活动数据'
    else:
        group_df = pd.concat(dfs)
        # 更新MySQL
        table = 'dis_activity_group_buy'
        del_sql = 'delete from {0} where ds="{1}"'.format(table, date)
        update_mysql(table, group_df, del_sql)
        print 'dis_activity_group_buy complete'


if __name__ == '__main__':
    settings_dev.set_env('dancer_mul')
    for date in date_range('20170707', '20170710'):
        print date
        act_crontract(date)
