#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''

'''
import settings_dev
from utils import hql_to_df, update_mysql, ds_add, date_range
from pandas import DataFrame


if __name__ == '__main__':
    settings_dev.set_env('dancer_tw')
    # seven_scripture()
# def seven_scripture():
    reward_sql = '''
    SELECT a_tar,server,ds
    FROM mid_actionlog
    WHERE a_typ = 'user.get_level_gift'
      AND ds >='20160907'
      AND ds <='20160917'
      AND server >= 'tw0'
      AND server <= 'tw2'
      AND return_code != 'error_4'
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


    sql = '''
    select ds,server,level,count(distinct user_id) from mid_actionlog
    WHERE ds >='20160907'
      AND ds <='20160917'
      AND server >= 'tw0'
      AND server <= 'tw2'
    group by ds,server,level
    order by ds,server,level
    '''
    df = hql_to_df(sql)

    result_df = result_df.rename(columns= {'reward_id':'level'})
    result_df['level'] = result_df['level'].map(lambda s: int(s))
    result = result_df.merge(df,on = ['ds','server','level'],how = 'right')
    result = result.fillna(0)

    print result
    result.to_excel('/Users/kaiqigu/Downloads/Excel/level_gift.xlsx')
