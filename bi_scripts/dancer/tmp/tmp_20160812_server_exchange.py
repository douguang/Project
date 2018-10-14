#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''

'''
import settings_dev
from utils import hql_to_df, update_mysql, ds_add, date_range
from pandas import DataFrame
def seven_scripture():
    reward_sql = '''
    SELECT user_id,
           a_tar
    FROM mid_actionlog
    WHERE ds = '20160814'
      AND a_typ = 'server_exchange.server_omni_exchange'
    '''
    reward_df = hql_to_df(reward_sql)
    user_id_list, reward_list = [], []
    for i in range(len(reward_df)):
        tar = reward_df.iloc[i,1]
        tar = eval(tar)
        user_id_list.append(reward_df.iloc[i,0])
        reward_list.append(tar['id'])
    mid_df = DataFrame({'user_id':user_id_list, 'reward_id':reward_list})
    print mid_df
    mid_df.to_excel('/Users/kaiqigu/Documents/dancer/tmp_20160814.xlsx')
if __name__ == '__main__':
    settings_dev.set_env('dancer_tw')
    seven_scripture()
