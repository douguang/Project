#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
@Time    : 17-6-27 下午4:20
@Author  : Andy 
@File    : gem_bag_data.py
@Software: PyCharm
Description :   宝石数据


'''


import settings_dev
import pandas as pd
from utils import hql_to_df,ds_add, get_config, date_range
import json

def gem_bag_data(start_ds,end_ds,start_time):
    print start_ds,end_ds,start_time
    # 近两个月活跃的玩家的神器数据
    user_info_sql = '''
     select user_id,level,vip,gem_dict  from mid_info_all where ds='{end_ds}' and act_time >= '{start_time}' and gem_dict != 'NULL'
    '''.format(start_ds=start_ds,end_ds=end_ds,start_time=start_time)
    print user_info_sql
    user_info_df = hql_to_df(user_info_sql)
    print user_info_df

    # 排除VIP12以上的
    result_df = user_info_df[user_info_df['vip'] <= 12]
    result_df = result_df[result_df['gem_dict'] != '{}']
    print result_df

    user_id_list, level_list, vip_list, key_list,value_list = [], [], [], [],[]
    for i in range(len(user_info_df)):
        # try:
            user_id = user_info_df.iloc[i, 0]
            level = user_info_df.iloc[i, 1]
            vip = user_info_df.iloc[i, 2]
            gem_dict = user_info_df.iloc[i, 3]
            print gem_dict
            gem_dict = json.loads(json.dumps(dict(eval(gem_dict))))
            for key in gem_dict.keys():
                print key
                if gem_dict != dict():
                    print '-----------'
                    user_id_list.append(user_id)
                    level_list.append(level)
                    vip_list.append(vip)
                    # if 'quality' in value.values():
                    #     quality = value[u'quality']
                    print key
                    print gem_dict[key]
                    key_list.append(key)
                    value_list.append(gem_dict[key])

    result_df = pd.DataFrame({'user_id': user_id_list,
                              'level': level_list,
                              'vip': vip_list,
                              'key': key_list,
                              'value':value_list})

    print result_df

    return result_df
if __name__ == '__main__':
    settings_dev.set_env('sanguo_tt')
    start_ds = ''
    end_ds = '20170629'
    start_time = '2017-04-13 00:00:00'
    user_info_df = gem_bag_data(start_ds,end_ds,start_time)
    user_info_df.to_excel('/home/kaiqigu/桌面/机甲无双-TT版-宝石数据_20170630.xlsx', index=False)
    print "end"