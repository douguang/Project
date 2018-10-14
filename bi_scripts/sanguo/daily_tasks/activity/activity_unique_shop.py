#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
@Time    : 16-11-25 下午2:52
@Author  : Andy
@File    : activity_unique_shop.py
@Software: PyCharm
Description : 无双商城  已完成
输出结果：ds	shop_id	buy_user_num count
        日期 商品ID   购买人数      购买次数
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
    SELECT ds,user_id,reverse(substr(reverse(user_id), 8)) as server,a_tar,return_code,vip_level
    FROM parse_actionlog
    WHERE ds = '{start_ds}'
    and ds<='{end_ds}'
    and a_typ = 'shop.unique_shop_buy'
    and return_code = ''
    '''.format(**{'start_ds': start_ds, 'end_ds': end_ds})
    df = hql_to_df(sql)
    print df.head(10)

    ds_list, user_id_list,server_id_list, shop_id_list, count_list, vip_list = [], [], [], [], [],[]
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
        server_id_list.append(server)
        shop_id_list.append(shop_id)
        count_list.append(count)
        vip_list.append(vip)

    data = DataFrame({'ds': ds_list,
                      'shop_id': shop_id_list,
                      'count': count_list,
                      'user_id': user_id_list,
                      'server': server_id_list,
                      'vip': vip_list})
    buy_num = data.groupby(['ds', 'server','shop_id']).agg(
        {'user_id': lambda g: g.nunique()}).reset_index()
    buy_num = buy_num.rename(columns={'user_id': 'buy_user_num', })
    count_num = data.groupby(['ds', 'server','shop_id']).agg(
        {'count': lambda g: g.sum()}).reset_index()
    count_num = count_num.rename(columns={'user_id': 'buy_num', })

    if df.__len__() == 0:
        return pd.DataFrame()
    else:
        result = DataFrame(buy_num).merge(
            count_num, on=['ds', 'server','shop_id'], how='left')
        result = DataFrame(result).fillna(0)
        print result.head(5)
        return result

if __name__ == '__main__':
    start_ds = '20170115'
    end_ds = '20170115'
    platform = 'sanguo_kr'
    result = unique_shop(platform, start_ds, end_ds)
    if result.__len__() != 0:
        result.to_excel(
            '/home/kaiqigu/桌面/%s_%s_%s_unique_shop.xlsx' %
            (platform, start_ds, end_ds), index=False)
    else:
        print "查询时间没有无双商城活动"
    print "end"
