#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
@Time    : 16-12-28 下午7:02
@Author  : Andy 
@File    : dis_activity_unique_shop.py
@Software: PyCharm
Description :
'''

from pandas import DataFrame
from utils import get_active_conf, hql_to_df, ds_short, update_mysql
import settings_dev
import time
def dis_activity_unique_shop(date):
    version, act_start_time, act_end_time = get_active_conf('unique',
                                                            date)
    if version == 2016:
        act_start_short = ds_short(act_start_time)
        act_end_short = ds_short(act_end_time)
        print version, act_start_time, act_end_time
        dis_unique_shop(act_start_short,act_end_short,act_start_time,act_end_time)
    else:
        print '{0} 没有无双商城活动'.format(date)


def dis_unique_shop(act_start_short,act_end_short,):
    sql = '''
    SELECT user_id,reverse(substr(reverse(user_id), 8)) as server,a_tar,return_code,vip_level
    FROM parse_actionlog
    WHERE ds >= '{act_start_short}'
    AND ds<='{act_end_short}'
    and a_typ = 'shop.unique_shop_buy'
    and return_code = ''
    '''.format(act_start_short=act_start_short,
               act_end_short=act_end_short,)
    df = hql_to_df(sql)
    df['ds']=act_start_short

    if df.__len__() != 0:
        ds_list, user_id_list,server_list, shop_id_list, count_list, vip_list = [], [], [], [], [],[]
        for i in range(len(df)):
            ds = df.iloc[i, 0]
            user_id = df.iloc[i, 1]
            server = df.iloc[i, 2]
            tar = df.iloc[i, 3]
            vip = df.iloc[i, 5]
            tar = eval(tar)
            shop_id = tar['shop_id']
            count = int(tar['count'])
            ds_list.append(ds)
            user_id_list.append(user_id)
            server_list.append(server)
            shop_id_list.append(shop_id)
            count_list.append(count)
            vip_list.append(vip)

        data = DataFrame({'ds': ds_list,
                          'shop_id': shop_id_list,
                          'count': count_list,
                          'user_id': user_id_list,
                          'server': server_list,
                          'vip': vip_list})
        buy_num = data.groupby(['ds', 'server','shop_id']).agg(
            {'user_id': lambda g: g.nunique()}).reset_index()
        buy_num = buy_num.rename(columns={'user_id': 'buy_user_num', })
        count_num = data.groupby(['ds', 'server','shop_id']).agg(
            {'count': lambda g: g.sum()}).reset_index()
        count_num = count_num.rename(columns={'user_id': 'buy_num', })

        result = DataFrame(buy_num).merge(
            count_num, on=['ds', 'server','shop_id'], how='left')
        result_df = DataFrame(result).fillna(0)

        result_df['ds'] = result_df['ds'].astype("str")
        result_df['server'] = result_df['server'].astype("str")
        result_df['shop_id'] = result_df['shop_id'].astype("int")
        result_df['buy_user_num'] = result_df['buy_user_num'].astype("int")
        result_df['count'] = result_df['count'].astype("int")
        table = 'dis_activity_unique_shop'
        del_sql = 'delete from {0} where ds="{1}"'.format(table, date)
        update_mysql(table, result_df, del_sql)

if __name__ == '__main__':
    date = '20161211'
    settings_dev.set_env('sanguo_ks')
    result = dis_activity_unique_shop(date)

