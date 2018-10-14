#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Dong Junshuang
Description : 20160912之前的info数据
'''
import settings_dev
from utils import hql_to_df, hqls_to_dfs
from pandas import DataFrame

if __name__ == '__main__':
    settings_dev.set_env('dancer_tw')
    date = '20160911'
    action_sql = '''
    SELECT user_id,
           a_tar
    FROM mid_actionlog
    WHERE ds = '{0}'
    '''.format(date)
    mid_info_sql = '''
    SELECT user_id ,
           account ,
           name ,
           reg_time ,
           act_time ,
           level ,
           vip ,
           free_coin ,
           charge_coin ,
           gold ,
           energy ,
           cmdr_energy ,
           honor ,
           combat ,
           guide ,
           max_stage ,
           item_dict ,
           card_dict ,
           equip_dict ,
           combiner_dict ,
           once_reward ,
           card_assistant ,
           combiner_in_use ,
           card_assis_active ,
           chips ,
           chip_pos ,
           equip_pos ,
           device_mark
    FROM mid_info_all
    WHERE ds = '{0}'
    '''.format(date)
    action_df, mid_info_df = hqls_to_dfs([action_sql, mid_info_sql])

    user_id_list, appid_list = [], []
    for _, row in action_df.iterrows():
        args = eval(row['a_tar'])
        if args.has_key('appid'):
            appid = args['appid']
        else:
            appid = ' '
        user_id_list.append(row['user_id'])
        appid_list.append(appid)

    result_df = DataFrame({'user_id': user_id_list, 'platform': appid_list})
    uid_info_df = result_df.groupby(['user_id']).max().platform.reset_index()

    result = uid_info_df.merge(mid_info_df, on='user_id', how='left')
    columns = [
        'user_id', 'account', 'name', 'reg_time', 'act_time', 'level', 'vip',
        'free_coin', 'charge_coin', 'gold', 'energy', 'cmdr_energy', 'honor',
        'combat', 'guide', 'max_stage', 'item_dict', 'card_dict', 'equip_dict',
        'combiner_dict', 'once_reward', 'card_assistant', 'combiner_in_use',
        'card_assis_active', 'chips', 'chip_pos', 'equip_pos', 'device_mark'
    ]
    result = result[columns]

    # 导出数据到指定文件
    result.to_csv(
        '/home/data/dancer_tw/redis_stats/raw_12ago_info_{date}'.format(
            date=date),
        sep='\t',
        index=False,
        head=False)

