#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''

'''
import settings_dev
from utils import hql_to_df, update_mysql, ds_add, date_range
from pandas import DataFrame
import pandas as pd


if __name__ == '__main__':
    settings_dev.set_env('dancer_tx_beta')

    # 获取水军数据
    df = pd.read_excel("/Users/kaiqigu/Downloads/Excel/water_uid.xlsx")
    # seven_scripture()
# def seven_scripture():
    reward_sql = '''
    SELECT a_tar,server,ds,user_id
    FROM mid_actionlog
    WHERE a_typ = 'user.get_level_gift'
      AND ds >='20160913'
      AND ds <='20160920'
      AND return_code != 'error_4'
    '''
    reward_df = hql_to_df(reward_sql)

    reward_df['is_shui'] = reward_df['user_id'].isin(df.user_id.values)
    reward_df = reward_df[~reward_df['is_shui']]

    reward_list = []
    account_list = []
    server_list = []
    ds_list = []
    for i in range(len(reward_df)):
        tar = reward_df.iloc[i,0]
        server = reward_df.iloc[i,1]
        ds = reward_df.iloc[i,2]
        tar = eval(tar)
        if tar.has_key('lv'):
            if tar.has_key('mobage_id'):
                account_list.append(tar['mobage_id'])
                reward_list.append(tar['lv'])
                server_list.append(server)
                ds_list.append(ds)
    mid_df = DataFrame({'account':account_list, 'reward_id':reward_list,'server':server_list,'ds':ds_list})
    mid_df = mid_df.drop_duplicates([ 'account','ds','reward_id','server'])
    result_df = mid_df.groupby(['ds','reward_id','server']).count().reset_index()


    sql = '''
    SELECT distinct ds,
           server,
           user_id,
           level
    FROM mid_actionlog
    WHERE ds >='20160913'
      AND ds <='20160920'
    '''
    act_df = hql_to_df(sql)

    act_df['is_shui'] = act_df['user_id'].isin(df.user_id.values)
    act_df = act_df[~act_df['is_shui']]

    act_result_df = act_df.groupby(['ds','server','level']).count().user_id.reset_index()

    result_df = result_df.rename(columns= {'reward_id':'level'})
    result_df['level'] = result_df['level'].map(lambda s: int(s))
    result = result_df.merge(act_result_df,on = ['ds','server','level'],how = 'right')
    result = result.fillna(0)

    # print result
    result.to_excel('/Users/kaiqigu/Downloads/Excel/level_gift.xlsx')
