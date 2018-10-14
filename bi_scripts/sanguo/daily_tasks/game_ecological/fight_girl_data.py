#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
@Time    : 17-6-27 下午3:02
@Author  : Andy 
@File    : fight_girl_data.py
@Software: PyCharm
Description :   战姬数据
'''


import settings_dev
import pandas as pd
from utils import hql_to_df,ds_add, get_config, date_range
import json

def fight_girl(start_ds,end_ds,start_time):
    print start_ds,end_ds,start_time
    # 近两个月活跃的玩家的神器数据
    user_info_sql = '''
    select user_id,level,vip,fight_girl from mid_info_all where ds='{end_ds}' and act_time >= '{start_time}'
    '''.format(start_ds=start_ds,end_ds=end_ds,start_time=start_time)
    print user_info_sql
    user_info_df = hql_to_df(user_info_sql)
    print user_info_df

    # 排除VIP12以上的
    user_info_df = user_info_df[user_info_df['vip'] <= 12]
    print user_info_df

    user_id_list, level_list, vip_list, girl_list, status_list, pre_exp_list, star_list, girl_level_list, current_level_list ,current_exp_list,exp_list= [], [], [], [],[], [], [], [],[],[],[]
    for i in range(len(user_info_df)):
        # try:
            user_id = user_info_df.iloc[i, 0]
            level = user_info_df.iloc[i, 1]
            vip = user_info_df.iloc[i, 2]
            fight_girl = user_info_df.iloc[i, 3]
            print '-----------'
            print fight_girl
            print type(fight_girl)
            if fight_girl != '{}':
                print "---"
                fight_girl = json.loads(json.dumps((eval(fight_girl))))
                print fight_girl

                print type(fight_girl)
                for key in fight_girl.keys():
                    girl =  key
                    girl_info = dict(fight_girl[key])

                    status = girl_info.get('status',0)
                    pre_exp = girl_info.get('pre_exp',0)
                    star = girl_info.get('star',0)
                    girl_level = girl_info.get('level',0)
                    current_level = girl_info.get('current_level',0)
                    current_exp = girl_info.get('current_exp',0)
                    exp = girl_info.get('exp',0)
                    print '************'
                    print girl_info.get('status', 0)
                    print girl_info.get('pre_exp', 0)
                    print girl_info.get('star', 0)
                    print girl_info.get('level', 0)
                    print girl_info.get('current_level', 0)
                    print girl_info.get('current_exp', 0)
                    print girl_info.get('exp', 0)

                    user_id_list.append(user_id)
                    level_list.append(level)
                    vip_list.append(vip)
                    girl_list.append(girl)
                    status_list.append(status)
                    pre_exp_list.append(pre_exp)
                    star_list.append(star)
                    girl_level_list.append(girl_level)
                    current_level_list.append(current_level)
                    current_exp_list.append(current_exp)
                    exp_list.append(exp)


                    # yield

                    # print fight_girl[key]
                # print fight_girl.index()
                # print fight_girl.
                # print fight_girl.__setattr__()

            # artifact_lv_list = ['1', '2', '3', '4', '5', '6', ]
            # artifact_lv = [artifact_lv[key] for key in artifact_lv_list]
            #
            # user_id_list.append(user_id)
            # level_list.append(level)
            # vip_list.append(vip)
            #
            #
            # artifact_lv_1.append(artifact_lv[0])
            # artifact_lv_2.append(artifact_lv[1])
            # artifact_lv_3.append(artifact_lv[2])
            # artifact_lv_4.append(artifact_lv[3])
            # artifact_lv_5.append(artifact_lv[4])
            # artifact_lv_6.append(artifact_lv[5])



        # except:
        #     pass

    result_df = pd.DataFrame({'user_id': user_id_list,
                              'level': level_list,
                              'vip': vip_list,
                              'girl': girl_list,
                              'status': status_list,
                              'pre_exp': pre_exp_list,
                              'star': star_list,
                              'girl_level': girl_level_list,
                              'current_level': current_level_list,
                              'current_exp': current_exp_list,
                              'exp': exp_list,})

    print ''
    # 分组统计

    print result_df.head()
    return result_df
if __name__ == '__main__':
    settings_dev.set_env('sanguo_tw')
    start_ds = ''
    end_ds = '20170626'
    start_time = '2017-04-13 00:00:00'
    user_info_df = fight_girl(start_ds,end_ds,start_time)
    user_info_df.to_excel('/home/kaiqigu/桌面/机甲无双-台湾版-战姬数据_20170627.xlsx', index=False)
    print "end"
