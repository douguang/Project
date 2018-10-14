#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
@Time    : 16-12-13 上午10:11
@Author  : Andy 
@File    : activity_normal_exchange.py
@Software: PyCharm
Description :
'''

import pandas as pd
from pandas import DataFrame
from utils import ds_add, hql_to_df, update_mysql, get_config, date_range
import settings_dev
from pandas import DataFrame
from utils import hql_to_df


def normal_exchange(platform):
    settings_dev.set_env(platform)
    sql = '''
        select ds,user_id,a_tar
        from parse_actionlog
        where ds>='20170629'
        and ds<='20170701'
        and log_t >= '1498752000'
        and log_t< '1498838400'
        and a_typ = 'normal_exchange.normal_exchange'
    '''
    #and a_typ = 'shop.unique_shop_buy'
    df = hql_to_df(sql)
    print df.head(10)

    ds_list, user_id_list, exchange_id_list= [], [], []
    for i in range(len(df)):
        ds = df.iloc[i, 0]
        user_id = df.iloc[i, 1]
        tar = df.iloc[i, 2]
        tar = eval(tar)
        exchange_id = tar['id']
        ds_list.append(ds)
        user_id_list.append(user_id)
        exchange_id_list.append(exchange_id)

    data = DataFrame({'ds': ds_list,
                      'user_id': user_id_list,
                      'exchange_id': exchange_id_list})
    # data = data[data['exchange_id'] == '284']
    data['exchange'] = 1

    buy_um = data.groupby(['ds', 'exchange_id',]).agg(
        {'exchange': lambda g: g.sum()}).reset_index()
    buy_um = buy_um.rename(columns={'exchange': 'count_buy_num', })

    buy_player_um = data.groupby(['ds', 'exchange_id',]).agg(
        {'user_id': lambda g: g.nunique()}).reset_index()
    buy_player_um = buy_player_um.rename(columns={'user_id': 'count_player_num', })

    result = DataFrame(buy_um).merge(
        buy_player_um, on=['ds', 'exchange_id',], how='left')
    result = DataFrame(result).fillna(0)

    return result

if __name__ == '__main__':
    platform = 'sanguo_tl'
    result = normal_exchange(platform)
    if result.__len__() != 0:
        result.to_excel(
            '/home/kaiqigu/桌面/机甲无双_多语言_normal_exchange_user_id.xlsx', index=False)
    else:
        print "查询时间没有normal_exchange活动"
    print "end"