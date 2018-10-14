#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
@Time    : 16-12-21 下午7:24
@Author  : Andy 
@File    : activity_server_foundation.py
@Software: PyCharm
Description :  卧龙基金的购买与领取日期
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
        select ds,user_id,a_typ,a_tar
        from parse_actionlog
        where ds>='20170630'
        and a_typ like '%foundation%'
        and (a_typ like 'withdraw%' or a_typ like '%activate%' )
        group by ds,user_id,a_typ,a_tar
    '''
    #and a_typ = 'shop.unique_shop_buy'
    print sql
    df = hql_to_df(sql)
    print df.head(10)

    ds_list, user_id_list, a_typ_list,a_tar_list= [], [], [],[]
    for i in range(len(df)):
        ds = df.iloc[i, 0]
        user_id = df.iloc[i, 1]
        a_typ = df.iloc[i, 2]

        if a_typ == 'server_foundation_new.withdraw' :
            a_typ = 1
        if a_typ == 'server_foundation_new.activate' :
            a_typ = 0

        tar = df.iloc[i, 3]
        tar = eval(tar)
        id = tar['id']


        ds_list.append(ds)
        user_id_list.append(user_id)
        a_typ_list.append(a_typ)
        a_tar_list.append(id)

    data = DataFrame({'ds': ds_list,
                      'user_id': user_id_list,
                      'a_typ': a_typ_list,
                      'id': a_tar_list})
    data['exchange_id'] = 1

    buy_um = data.groupby(['ds', 'a_typ', 'id']).agg(
        {'exchange_id': lambda g: g.sum()}).reset_index()
    buy_um = buy_um.rename(columns={'exchange_id': 'count_buy_num', })

    buy_player_um = data.groupby(['ds', 'a_typ', 'id']).agg(
        {'user_id': lambda g: g.nunique()}).reset_index()
    buy_player_um = buy_player_um.rename(columns={'user_id': 'count_player_num', })

    result = DataFrame(buy_um).merge(
        buy_player_um, on=['ds', 'a_typ', 'id'], how='left')
    result = DataFrame(result).fillna(0)

    return result

if __name__ == '__main__':
    platform = 'sanguo_tl'
    result = normal_exchange(platform)
    if result.__len__() != 0:
        result.to_excel(
            '/home/kaiqigu/桌面/机甲无双_多语言_卧龙基金的购买人数.xlsx', index=False)
    else:
        print "查询时间没有活动"
    print "end"
