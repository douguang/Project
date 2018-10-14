#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Zhang Yongchen
Description :
求跑下11月24日-28日  全服累计储值各档位领取人数  ID474-480
Database    : dancer_tw
'''
import settings_dev
import pandas as pd
from utils import hql_to_df
def tmp_20161128_active_recharge():

    # 累积储值
    recharge_sql = '''
        select
            ds,
            server,
            user_id,
            a_tar
        from
            parse_actionlog
        where
            ds>='20170125' and ds<='20170205' and
            a_typ='active.active_consume_receive'
      '''
    print recharge_sql
    recharge_df = hql_to_df(recharge_sql)
    print recharge_df.head(10)

    user_id_list, server_list, id_list, ds_list = [], [], [], []
    for i in range(len(recharge_df)):
        user_id_list.append(recharge_df.iloc[i, 2])
        ds_list.append(recharge_df.iloc[i, 0])
        server_list.append(recharge_df.iloc[i, 1])
        tar = recharge_df.iloc[i, 3]
        tar = eval(tar)
        id_list.append(tar['active_id'])

    data = pd.DataFrame({'user_id': user_id_list, 'server': server_list, 'id': id_list, 'ds': ds_list})

    data['num'] = 1
    result = data.groupby(['server', 'ds', 'id', 'user_id']).agg({
        'num': lambda g: g.sum()
    }).reset_index()

    return result


if __name__ == '__main__':
    settings_dev.set_env('dancer_tw')
    result = tmp_20161128_active_recharge()
    result.to_excel('/home/kaiqigu/Documents/active.active_consume_receive.xlsx')
