#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Zhang Yongchen
Description : 20161130日    omni_exchange  的 兑换次数:UID  | 角色创建时间  | 玩家VIP等级 | 当前剩余元宝|兑换A道具次数|兑换B道具次数
Database    : dancer_tw
'''
import settings_dev
import pandas as pd
from utils import hql_to_df
def tmp_20161128_exchange():

    #全服兑换

    # exchange_sql = '''
    #     select
    #         ds,
    #         server,
    #         vip,
    #         user_id,
    #         a_tar
    #     from
    #         parse_actionlog
    #     where
    #         ds>='20170401' and ds<='20170404' and
    #         a_typ in ('omni_exchange.omni_exchange','server_exchange.server_omni_exchange')
    # '''
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
                ds>='20170401' and ds<='20170404' and
                a_typ='active.active_consume_receive'
        '''
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
        id_list.append(tar['active_id'])

    data = pd.DataFrame({'user_id': user_id_list, 'server': server_list, 'vip': vip_list, 'id': id_list, 'ds': ds_list})

    data['num'] = 1
    result = data.groupby(['user_id', 'server', 'vip', 'ds', 'id']).agg({
        'num':lambda g:g.count()
    }).reset_index()

    return result


if __name__ == '__main__':
    settings_dev.set_env('dancer_tw')
    result = tmp_20161128_exchange()
    result.to_excel(r'E:\Data\output\dancer\omni_exchange.xlsx')
