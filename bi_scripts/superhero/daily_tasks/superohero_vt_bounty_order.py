#!/usr/bin/env python  
# encoding: utf-8  

""" 
@author: Andy 
@site:  赏金令
@software: PyCharm 
@file: superohero_vt_bounty_order.py 
@time: 17/9/4 下午12:00 
"""


import settings_dev
from utils import ds_add
from utils import hql_to_df
from utils import update_mysql
from sqls_for_games.superhero import gs_sql
import pandas as pd


def data_reduce(date):
    account_uid_sql = '''
        select account,uid from mid_info_all where ds='20170903' group by account,uid
    '''.format(date=date,)
    account_uid_df = hql_to_df(account_uid_sql)
    print account_uid_df.head(3)


    account_max_level_sql = '''
        select account,max(vip_level) as vip from mid_info_all where ds='20170903' group by account
    '''.format(date=date,)
    account_max_level_df = hql_to_df(canyu_sql)
    print account_max_level_df.head(3)

    play_sql = '''
        select uid,pre_vip as vip,
        case when action='roulette.open_roulette' then 'yi'  
                                   when action='roulette.open_roulette10' then 'shi' 
                                   when action='roulette.refresh' then 'shua' else 'index' end as jiekuo
        from raw_action_log where ds='{date}'  group by uid,vip,jiekuo
    '''.format(date=date,)
    play_df = hql_to_df(play_sql)
    print play_df.head(3)
    user_id_list, server_id_list, refresh_list, times_list = [], [], [], []
    for i in range(len(play_df)):
        user_id = play_df.iloc[i, 0]
        server = play_df.iloc[i, 1]
        a_typ = play_df.iloc[i, 2]

        if a_typ == 'yi':
            refresh = 0
            times = 1
        elif a_typ == 'shi':
            refresh = 0
            times = 10
        elif a_typ == 'shua':
            refresh = 1
            times = 0
        else:
            refresh = 0
            times = 0

        user_id_list.append(user_id)
        server_id_list.append(server)
        refresh_list.append(refresh)
        times_list.append(times)

    play_df = pd.DataFrame({'uid': user_id_list,
                      'vip': server_id_list,
                      'refresh': refresh_list,
                      'times': times_list,})

    play_df = play_df.groupby(['uid', 'vip']).agg({'refresh': lambda g: g.sum(),'times': lambda g: g.sum()}).reset_index()
    play_df = play_df.groupby(['vip',]).agg({'refresh': lambda g: g.sum(), 'times': lambda g: g.sum(),'uid': lambda g: g.nunique()}).reset_index()
    play_df = play_df.rename(columns={'uid': 'canyu_num', })
    print play_df.head(4)

    spendlog_sql = '''
        select t2.vip,sum(t1.coin_num) as coin_num from (
          select  uid,sum(coin_num) as coin_num from raw_spendlog where ds='{date}' group by uid
        )t1 left outer join(
          select uid,vip_level as vip from mid_info_all where ds='{date}' group by uid,vip_level
        )t2 on t1.uid=t2.uid
        group by  t2.vip
    '''.format(date=date,)
    spendlog_df = hql_to_df(spendlog_sql)
    print spendlog_df.head(3)

    result = dau_df.merge(canyu_df, on=['vip',], how='left')
    result = result.merge(play_df, on=['vip',], how='left')
    result = result.merge(spendlog_df, on=['vip',], how='left')

    return result


if __name__ == '__main__':
    settings_dev.set_env('superhero_vt')
    date = '20170827'
    result = data_reduce(date)
    result.to_excel('/Users/kaiqigu/Documents/Sanguo/超级英雄-越南-幸运轮盘数据_%s.xlsx' % date)