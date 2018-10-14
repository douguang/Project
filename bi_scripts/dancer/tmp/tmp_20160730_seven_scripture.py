#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Hu Wenchao
Description : 3日活跃人数汇总表
Name        : dis_d3_act_user_num
'''
import settings_dev
from utils import hql_to_df, update_mysql, ds_add, date_range
from pandas import DataFrame
def seven_scripture():
    reg_sql = '''
    SELECT user_id
    FROM parse_info
    WHERE ds = '20160808'
    '''
    # reward_sql = '''
    # SELECT user_id,
    #        a_tar
    # FROM mid_actionlog
    # WHERE ds >= '20160808'
    #   AND ds <= '20160815'
    #   AND a_typ = 'seven_scripture.get_reward'
    # '''
    shop_sql = '''
    SELECT user_id,
           a_tar
    FROM mid_actionlog
    WHERE ds >= '20160808'
      AND ds <= '20160815'
      AND a_typ = 'seven_scripture.buy'
      AND (freemoney_diff < 0 or money_diff <0)
    '''

    shop_df = hql_to_df(shop_sql)
    reg_df = hql_to_df(reg_sql)
    # reward_df = hql_to_df(reward_sql)
    user_id_list, reward_list = [], []
    for i in range(len(shop_df)):
        tar = shop_df.iloc[i,1]
        tar = eval(tar)
        user_id_list.append(shop_df.iloc[i,0])
        reward_list.append(tar['shop_id'])
    mid_df = DataFrame({'user_id':user_id_list, 'shop_id':reward_list})
    print mid_df
    result_df = mid_df.merge(reg_df,on='user_id',how='inner')
    group_df = result_df.groupby('shop_id').count().reset_index()
    print group_df
    group_df.to_excel('/Users/kaiqigu/Documents/dancer/tmp_20160819_seven_scripture_shop_id.xlsx')
if __name__ == '__main__':
    settings_dev.set_env('dancer_tw')
    seven_scripture()
