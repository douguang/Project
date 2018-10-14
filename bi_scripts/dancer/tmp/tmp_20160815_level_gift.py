#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''

'''
import settings_dev
from utils import hql_to_df
from pandas import DataFrame


if __name__ == '__main__':
    settings_dev.set_env('dancer_tw')
    # seven_scripture()
# def seven_scripture():
    reward_sql = '''
    SELECT a_tar,server,ds
    FROM mid_actionlog
    WHERE a_typ = 'user.get_level_gift'
      AND ds >='20160913'
      AND ds <='20160919'
      AND return_code = ''
    '''
    reward_df = hql_to_df(reward_sql)
    reward_list = []
    account_list = []
    server_list = []
    ds_list = []
    for i in range(len(reward_df)):
        tar = reward_df.iloc[i,0]
        server = reward_df.iloc[i,1]
        ds = reward_df.iloc[i,2]
        if 'lv' in tar:
            if 'mobage_id' in tar:
                tar = eval(tar)
                account_list.append(tar['mobage_id'])
                reward_list.append(tar['lv'])
                server_list.append(server)
                ds_list.append(ds)
    mid_df = DataFrame({'account':account_list, 'reward_id':reward_list,'server':server_list,'ds':ds_list})
    result_df = mid_df.groupby(['ds','reward_id','server']).count().reset_index()
    print result_df
    result_df.to_excel('/Users/kaiqigu/Downloads/Excel/level_gift.xlsx')
