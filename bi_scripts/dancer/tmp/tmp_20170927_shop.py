#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Zhang Yongchen
Description :  开服前七日的游戏数据，统计玩家为7日后尚未流失的玩家。
每日传奇商店，史诗商店，声望商店，五行商店，功勋商城，贡献商城，桃花商城（无刷新）的购买商城ID，次数，刷新商店次数
Database    : dancer_pub
'''
import settings_dev
import pandas as pd
from utils import hql_to_df, ds_add, date_range, hqls_to_dfs
from ipip import *
import numpy as np
from dancer.cfg import mul_server_date

# a_typ_dict = ('cards.mix', 'gacha.get_gacha', 'equip_gacha.get_gacha', 'shop.spark_shop_buy', 'shop.epic_shop_buy', 'gacha.get_all_gacha', 'equip_gacha.get_all_gacha')
# a_typ_dict = ('shop.pvp_shop_buy', 'blood_war.buy', 'hollow.hollow_shop_buy', 'shop.quin_shop_buy', 'shop.exploit_shop_buy', 'shop.shop_buy', 'arena.get_point_reward',
# 'arena.battle', 'blood_war.fight', 'blood_war.quickly_challenge', 'hollow.fight', 'hollow.quick_fight', 'fivefaces.fight', 'fivefaces.complete', 'active.forever_fight', 'active.auto_forever_fight')

def tmp_30_liucun_list():

    sql = '''
        select user_id, reverse(substr(reverse(user_id), 8)) as server, regexp_replace(to_date(reg_time), '-', '') as regtime, regexp_replace(to_date(act_time), '-', '') as acttime from mid_info_all where ds='20170926'
        and reverse(substr(reverse(user_id), 8)) in ('pm40', 'pm41', 'pm43', 'pm44')
    '''
    print sql
    df = hql_to_df(sql)
    df['server_date'] = df['server'].map(mul_server_date)
    print df.head(5)
    df['day'] = (pd.to_datetime(df['acttime']) - pd.to_datetime(df['regtime'])).dt.days
    df = df[df['day'] >= 7]
    df['server_day'] = (pd.to_datetime(df['regtime']) - pd.to_datetime(df['server_date'])).dt.days
    df = df[df['server_day'] <= 6]
    print df.head(5)
    train_data = np.array(df['user_id'])
    result_list = train_data.tolist()
    return result_list


if __name__ == '__main__':
    settings_dev.set_env('dancer_mul')
    user_list = tuple(tmp_30_liucun_list())
    sql = '''
    select t1.user_id, t2.vip, t2.regtime, t1.a_typ, t1.a_tar, t1.ds from (
        select user_id, a_typ, a_tar, ds from parse_actionlog where ds>='20170905' and user_id in {user_list} and a_typ like '%shop_buy%' and (a_tar like '%shop_id%' or a_tar like '%pos_id%')) t1 left join
        (select user_id, vip, regexp_replace(to_Date(reg_time), '-', '') as regtime from mid_info_all where ds='20170926') t2
        on t1.user_id = t2.user_id
    '''.format(user_list=user_list)
    print sql
    result = hql_to_df(sql)
    result['day'] = (pd.to_datetime(result['ds']) - pd.to_datetime(result['regtime'])).dt.days
    # result = result[result['day'] <= 29]
    print result.head(10)
    # result.to_excel(r'E:\Data\output\dancer\before123131.xlsx')
    print len(result)
    user_id_list, vip_list, regtime_list, a_typ_list, shop_id_list, day_list = [], [], [], [], [], []
    for i in range(len(result)):
        tar = eval(result.iloc[i, 4])
        if 'shop_id' in str(tar):
            shop_id = tar['shop_id']
        else:
            shop_id = tar['pos_id']
        shop_id_list.append(shop_id)
        user_id_list.append(result.iloc[i, 0])
        vip_list.append(result.iloc[i, 1])
        regtime_list.append(result.iloc[i, 2])
        a_typ_list.append(result.iloc[i, 3])
        day_list.append(result.iloc[i, 6])

    data = pd.DataFrame(
        {'user_id': user_id_list, 'vip': vip_list, 'regtime': regtime_list, 'a_typ': a_typ_list, 'shop_id': shop_id_list, 'day': day_list}).fillna(0)
    columns = ['user_id', 'vip', 'regtime', 'a_typ', 'shop_id', 'day']
    data = data[columns]
    print data.head(5)
    data.to_excel(r'E:\Data\output\dancer\shop_id.xlsx')

    # sql = '''
    #     select t1.user_id, t2.vip, t2.regtime, t1.a_typ, t1.ds from (
    #         select user_id, a_typ, ds from parse_actionlog where ds>='20170905' and user_id in {user_list} and a_typ like '%shop.refresh%') t1 left join
    #         (select user_id, vip, regexp_replace(to_Date(reg_time), '-', '') as regtime from mid_info_all where ds='20170926') t2
    #         on t1.user_id = t2.user_id
    #     '''.format(user_list=user_list)
    # print sql
    # result = hql_to_df(sql)
    # result['day'] = (pd.to_datetime(result['ds']) - pd.to_datetime(result['regtime'])).dt.days
    #
    # result.to_excel(r'E:\Data\output\dancer\shop_refresh.xlsx')