#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
@Time    : 16-12-6 上午12:20
@Author  : Andy
@File    : activity_do_limit_gacha.py
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
    SELECT ds,user_id,reverse(substr(reverse(user_id), 8)) as service_id,a_tar,return_code,vip_level
    FROM parse_actionlog
    WHERE ds >= '{start_ds}'
    and ds<='{end_ds}'
    and return_code = ''
    and a_typ = "card_gacha.do_gacha"
    and a_typ not like '%limit_gacha%'
    and log_t >='1484582400'
    and log_t <='1484622000'
    '''.format(**{'start_ds': start_ds, 'end_ds': end_ds})
    df = hql_to_df(sql)
    print df.head(10)

    ds_list, user_id_list, service_id_list,shop_id_list, count_list, vip_list = [], [], [], [], [],[]
    for i in range(len(df)):
        ds = df.iloc[i, 0]
        user_id = df.iloc[i, 1]
        service_id = df.iloc[i, 2]
        tar = df.iloc[i, 3]
        vip = df.iloc[i, 5]
        tar = eval(tar)
        shop_id = tar['gacha_id']
        count = int(tar['times'])
        ds_list.append(ds)
        user_id_list.append(user_id)
        service_id_list.append(service_id)
        shop_id_list.append(shop_id)
        count_list.append(count)
        vip_list.append(vip)

    data = DataFrame({'ds': ds_list,
                      'gacha_id': shop_id_list,
                      'count': count_list,
                      'user_id': user_id_list,
                      'service_id': service_id_list,
                      'vip': vip_list})
    print data.head(5)
    data.to_excel(
        '/home/kaiqigu/桌面/机甲无双-韩国_do_gacha.xlsx', index=False)
    buy_num = data.groupby(['ds', 'service_id','gacha_id']).agg(
        {'user_id': lambda g: g.nunique()}).reset_index()
    buy_num = buy_num.rename(columns={'user_id': 'recruiting_user_num', })
    count_num = data.groupby(['ds','service_id', 'gacha_id']).agg(
        {'count': lambda g: g.sum()}).reset_index()
    count_num = count_num.rename(columns={'count': 'recruiting_num', })

    if df.__len__() == 0:
        return pd.DataFrame()
    else:
        result = DataFrame(buy_num).merge(
            count_num, on=['ds', 'service_id','gacha_id'], how='left')
        result = DataFrame(result).fillna(0)
        print result.head(5)
        return result

if __name__ == '__main__':
    start_ds = '20170117'
    end_ds = '20170117'
    platform = 'sanguo_kr'
    result = unique_shop(platform, start_ds, end_ds)
    if result.__len__() != 0:
        result.to_excel(
            '/home/kaiqigu/桌面/%s_%s_%s_do_gacha.xlsx' %
            (platform, start_ds, end_ds), index=False)
    else:
        print "查询时间没有招募活动"
    print "end"