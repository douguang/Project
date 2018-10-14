#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Zhang Yongchen
Description : 跑下tw0-tw16服玩家这两天参与无量宝藏消耗的元宝数量：参与活动玩家总数，消耗的元宝数量，玩家参与消耗的库排行
Database    : dancer_tw
'''
import settings_dev
import pandas as pd
from utils import hql_to_df
def tmp_20161031_oracle(date):
    oracle_sql = '''
    select
      user_id,freemoney_diff,server,ds,a_tar,money_diff
    from
      parse_actionlog
    where
      ds = '{0}' and
      a_typ in ('server_oracle_reward.refresh','oracle_reward.refresh','server_oracle_reward.predict','oracle_reward.predict')
    '''.format(date)
    print oracle_sql
    oracle_df = hql_to_df(oracle_sql)

    user_id_list, freemoney_diff_list, server_list, ku_list, ds_list, money_diff_list = [],[],[],[],[],[]
    for i in range(len(oracle_df)):
        user_id_list.append(oracle_df.iloc[i,0])
        freemoney_diff_list.append(oracle_df.iloc[i,1])
        server_list.append(oracle_df.iloc[i,2])
        ds_list.append(oracle_df.iloc[i,3])
        money_diff_list.append(oracle_df.iloc[i, 5])
        tar = oracle_df.iloc[i,4]
        tar = eval(tar)
        ku_list.append(tar['oracle'])

    data = pd.DataFrame({'user_id': user_id_list, 'freemoney_diff': freemoney_diff_list, 'money_diff': money_diff_list, 'server': server_list, 'oracle': ku_list, 'ds': ds_list})

    return data


if __name__ == '__main__':
    settings_dev.set_env('dancer_tw')
    # date = ('20161027','20161103')
    date = '20161211'
    result = tmp_20161031_oracle(date)
    result.to_excel('/home/kaiqigu/Documents/oracle.xlsx')
