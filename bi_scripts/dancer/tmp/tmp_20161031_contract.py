#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Zhang Yongchen
Description : 跑下T0-T6 服玩家這兩天参与北冥之灵的玩家消耗元宝量总数（龙符，天符，风雷符），道具兑换情况（兑换道具项ID，兑换总次数）
Database    : dancer_tw
'''
import settings_dev
import pandas as pd
from utils import hql_to_df
def tmp_20161031_contract(date):
    contract_sql = '''
    select
      user_id, freemoney_diff, money_diff, server, ds, a_tar
    from
      parse_actionlog
    where
      ds in {0} and a_typ in
      ('server_magic_school.open_contract','magic_school.open_contract')
    '''.format(date)
    print contract_sql
    oracle_df = hql_to_df(contract_sql)

    user_id_list, freemoney_diff_list, money_diff_list, server_list, contract_id_list, ds_list = [], [], [], [], [], []
    for i in range(len(oracle_df)):
        user_id_list.append(oracle_df.iloc[i, 0])
        freemoney_diff_list.append(oracle_df.iloc[i, 1])
        money_diff_list.append(oracle_df.iloc[i, 2])
        server_list.append(oracle_df.iloc[i, 3])
        ds_list.append(oracle_df.iloc[i, 4])
        tar = oracle_df.iloc[i, 5]
        tar = eval(tar)
        contract_id_list.append(tar['contract_id'])


    data = pd.DataFrame({'user_id': user_id_list, 'freemoney_diff': freemoney_diff_list, 'money_diff': money_diff_list, 'server': server_list, 'contract_id': contract_id_list, 'ds': ds_list})

    return data


if __name__ == '__main__':
    settings_dev.set_env('dancer_tw')
    date = ('20170217','20170220')
    result = tmp_20161031_contract(date)
    result.to_excel(r'E:\Data\output\dancer\contract.xlsx')
