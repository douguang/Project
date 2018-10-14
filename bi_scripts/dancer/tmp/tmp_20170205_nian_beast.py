#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Zhang Yongchen
Description : 春节年兽活动,各种类型打年兽的次数
Database    : dancer_tw
'''
import settings_dev
import pandas as pd
from utils import hql_to_df, date_range
def tmp_20161128_exchange(date):

    # 年兽活动

    exchange_sql = '''
        select
            ds,
            server,
            vip,
            user_id,
            a_tar
        from
            parse_actionlog
        where
            ds='{date}' and
            a_typ='nian_beast.attack' and a_tar like '%sort%'
    '''.format(date=date)
    print exchange_sql
    exchange_df = hql_to_df(exchange_sql)

    user_id_list, server_list, vip_list, id_list, ds_list = [], [], [], [], []
    for i in range(len(exchange_df)):
        user_id_list.append(exchange_df.iloc[i, 3])
        ds_list.append(exchange_df.iloc[i, 0])
        server_list.append(exchange_df.iloc[i, 1])
        vip_list.append(exchange_df.iloc[i, 2])
        tar = exchange_df.iloc[i, 4]
        tar = eval(tar)
        sort = tar['sort']
        # print sort
        id_list.append(sort)

    data = pd.DataFrame({'user_id': user_id_list, 'server': server_list, 'vip': vip_list, 'id': id_list, 'ds': ds_list})

    data['num'] = 1
    result = data.groupby(['user_id', 'server', 'vip', 'ds', 'id']).agg({
        'num':lambda g:g.count()
    }).reset_index()

    return result


if __name__ == '__main__':
    settings_dev.set_env('dancer_tw')
    for date in date_range('20170205', '20170205'):
        result = tmp_20161128_exchange(date)
        result.to_csv('/home/kaiqigu/Documents/nianshou_%s.csv'%date)