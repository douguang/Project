#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
@Time    : 16-12-29 下午2:47
@Author  : Andy 
@File    : dis_activity_group_buy.py
@Software: PyCharm
Description :   团购活动
group_buy.group_active_buy	{u'count': u'1', u'devicename': u'GN9010L', u'item': u'2', u'channel_id': u'', u'identifier': u'868682020515793'}
'''

import pandas as pd
from pandas import DataFrame
from utils import get_active_conf, hql_to_df, ds_short, update_mysql, date_range
import settings_dev
import time


def dis_activity_group_buy(date):
    version, act_start_time, act_end_time = get_active_conf('group_version',
                                                            date)
    if version != '':
        act_start_short = ds_short(act_start_time)
        act_end_short = ds_short(act_end_time)
        print version, act_start_time, act_end_time
        dis_group_buy(act_start_short, act_end_short, date)
    else:
        print '{0} 没有团购活动'.format(date)


def dis_group_buy(act_start_short, act_end_short, date):
    sql = '''
        select user_id,reverse(substr(reverse(user_id), 8)) as server,a_tar
        from parse_actionlog
        WHERE ds >= '{act_start_short}'
        AND ds<='{act_end_short}'
        and a_typ like  '%group_active_buy%'
        and a_tar like '%item%'
    '''.format(act_start_short=act_start_short,
               act_end_short=act_end_short,)
    df = hql_to_df(sql)
    df = df.fillna(0)
    df['ds'] = act_start_short

    if df.__len__() != 0:
        ds_list, user_id_list, server_list, item_id_list, count_list = [], [], [], [], []
        for i in range(len(df)):
            ds = df.iloc[i, 3]
            user_id = df.iloc[i, 0]
            server = df.iloc[i, 1]
            tar = df.iloc[i, 2]
            # print tar
            tar = eval(tar)
            item_id = tar[u'item']
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
        buy_num = buy_num.rename(columns={'user_id': 'buy_user_num', })
        count_num = data.groupby(['ds', 'server', 'item_id']).agg(
            {'count': lambda g: g.sum()}).reset_index()
        count_num = count_num.rename(columns={'user_id': 'buy_num', })

        result = DataFrame(buy_num).merge(
            count_num, on=['ds', 'server', 'item_id'], how='left')
        result_df = DataFrame(result).fillna(0)
        # print result_df.head(5)

        result_df['ds'] = result_df['ds'].astype("str")
        result_df['server'] = result_df['server'].astype("str")
        result_df['item_id'] = result_df['item_id'].astype("int")
        result_df['buy_user_num'] = result_df['buy_user_num'].astype("int")
        result_df['count'] = result_df['count'].astype("int")
        # print result_df
        table = 'dis_activity_group_buy'
        del_sql = 'delete from {0} where ds="{1}"'.format(table, date)
        update_mysql(table, result_df, del_sql)

if __name__ == '__main__':
    # date = '20161105'
    # settings_dev.set_env('sanguo_ks')
    # result = dis_activity_group_buy(date)
    for platform in ['sanguo_tl',]:
        settings_dev.set_env(platform)
        for date in date_range('20170201', '20170304'):
            result = dis_activity_group_buy(date)
    print "end"
