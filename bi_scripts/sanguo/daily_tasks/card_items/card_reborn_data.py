#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
@Time    : 17-2-7 下午3:38
@Author  : Andy 
@File    : card_reborn_data.py
@Software: PyCharm
Description :
'''

import pandas as pd
from pandas import DataFrame
from utils import ds_add, hql_to_df, update_mysql, get_config, date_range
import settings_dev
from pandas import DataFrame
from utils import hql_to_df


def unique_shop(platform, start_ds, end_ds):
    settings_dev.set_env(platform)
    sql = '''
    SELECT ds,user_id,reverse(substr(reverse(user_id), 8)) as server,a_tar,return_code,vip_level,log_t
    FROM parse_actionlog
    WHERE ds >= '{start_ds}'
    and ds<='{end_ds}'
    and a_typ = 'cards.reborn'
    and return_code = ''
    '''.format(**{'start_ds': start_ds, 'end_ds': end_ds})
    df = hql_to_df(sql)
    print df.head(10)

    ds_list, user_id_list,server_id_list, card_id_list, skill_id_list, vip_list,log_t_list= [], [], [], [], [],[],[]
    for i in range(len(df)):
        ds = df.iloc[i, 0]
        user_id = df.iloc[i, 1]
        server = df.iloc[i, 2]
        tar = df.iloc[i, 3]
        vip = df.iloc[i, 5]
        log_t = df.iloc[i, 6]
        tar = eval(tar)
        card_id = tar['card_id']
        #skill_id = int(tar['skill_id'])
        ds_list.append(ds)
        user_id_list.append(user_id)
        server_id_list.append(server)
        card_id_list.append(card_id)
        #skill_id_list.append(skill_id)
        vip_list.append(vip)
        log_t_list.append(log_t)

    data = DataFrame({'ds': ds_list,
                      'card_id': card_id_list,
                      'user_id': user_id_list,
                      'server': server_id_list,
                      'vip': vip_list,
                      'log_t':log_t_list})
    # buy_num = data.groupby(['ds', 'server','shop_id']).agg(
    #     {'user_id': lambda g: g.nunique()}).reset_index()
    # buy_num = buy_num.rename(columns={'user_id': 'buy_user_num', })
    # count_num = data.groupby(['ds', 'server','shop_id']).agg(
    #     {'count': lambda g: g.sum()}).reset_index()
    # count_num = count_num.rename(columns={'user_id': 'buy_num', })

    if df.__len__() == 0:
        return pd.DataFrame()
    else:
        # result = DataFrame(buy_num).merge(
        #     count_num, on=['ds', 'server','shop_id'], how='left')
        result = DataFrame(data).fillna(0)
        print result.head(5)
        return result

if __name__ == '__main__':
    start_ds = '20170126'
    end_ds = '20170206'
    platform = 'sanguo_tw'
    result = unique_shop(platform, start_ds, end_ds)
    if result.__len__() != 0:
        result.to_excel(
            '/home/kaiqigu/桌面/%s_%s_%s_cards_reborn.xlsx' %
            (platform, start_ds, end_ds), index=False)
    else:
        print "查询时间没有卡牌重生数据"
    print "end"

