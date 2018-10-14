#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
@Time    : 17-6-27 上午10:51
@Author  : Andy 
@File    : artifact_lv_data.py
@Software: PyCharm
Description :   神器数据    神器总共6种

神器等级-神器ID1-神器ID2-神器ID3-神器ID4-神器ID5-神器ID6
0_10-人数(VIP0-VIP12)-人数(VIP0-VIP12)-人数(VIP0-VIP12)-人数(VIP0-VIP12)-人数(VIP0-VIP12)-人数(VIP0-VIP12)
11_20-人数(VIP0-VIP12)-人数(VIP0-VIP12)-人数(VIP0-VIP12)-人数(VIP0-VIP12)-人数(VIP0-VIP12)-人数(VIP0-VIP12)
...

'''

import settings_dev
import pandas as pd
from utils import hql_to_df,ds_add, get_config, date_range
import json

def artifact_lv_data(start_ds,end_ds,start_time):
    print start_ds,end_ds,start_time
    # 近两个月活跃的玩家的神器数据
    user_info_sql = '''
    select user_id,level,vip,artifact_lv from mid_info_all where ds='{end_ds}' and act_time >= '{start_time}'
    '''.format(start_ds=start_ds,end_ds=end_ds,start_time=start_time)
    print user_info_sql
    user_info_df = hql_to_df(user_info_sql)
    print user_info_df

    # 排除VIP12以上的
    user_info_df = user_info_df[user_info_df['vip'] <= 12]
    print user_info_df

    user_id_list, level_list, vip_list, artifact_lv_1, artifact_lv_2, artifact_lv_3, artifact_lv_4, artifact_lv_5, artifact_lv_6 = [], [], [], [],[], [], [], [],[]
    for i in range(len(user_info_df)):
        # try:
            user_id = user_info_df.iloc[i, 0]
            level = user_info_df.iloc[i, 1]
            vip = user_info_df.iloc[i, 2]
            artifact_lv = user_info_df.iloc[i, 3]
            artifact_lv = json.loads(json.dumps(dict(eval(artifact_lv))))
            artifact_lv_list = ['1', '2', '3', '4', '5', '6', ]
            artifact_lv = [artifact_lv[key] for key in artifact_lv_list]

            user_id_list.append(user_id)
            level_list.append(level)
            vip_list.append(vip)


            artifact_lv_1.append(artifact_lv[0])
            artifact_lv_2.append(artifact_lv[1])
            artifact_lv_3.append(artifact_lv[2])
            artifact_lv_4.append(artifact_lv[3])
            artifact_lv_5.append(artifact_lv[4])
            artifact_lv_6.append(artifact_lv[5])



        # except:
        #     pass

    result_df = pd.DataFrame({'user_id': user_id_list,
                              'level': level_list,
                              'vip': vip_list,
                              'artifact_lv_1': artifact_lv_1,
                              'artifact_lv_2': artifact_lv_2,
                              'artifact_lv_3': artifact_lv_3,
                              'artifact_lv_4': artifact_lv_4,
                              'artifact_lv_5': artifact_lv_5,
                              'artifact_lv_6': artifact_lv_6, })

    print result_df
    # 分组统计
    a_list =[]
    for a in list(result_df.artifact_lv_1):
        if a in range(1,11):a_list.append('A')
        if a in range(11,21):a_list.append('B')
        if a in range(21,31):a_list.append('C')
        if a in range(31,41):a_list.append('D')
        if a in range(41,51):a_list.append('E')
        if a in range(51,61):a_list.append('F')
        if a in range(61,71):a_list.append('G')
        if a in range(71,81):a_list.append('H')
        if a in range(81,91):a_list.append('I')
        if a in range(91,101):a_list.append('J')
        if a in range(101,111):a_list.append('K')
        if a in range(111,121):a_list.append('L')
        if a in range(121,131):a_list.append('M')
        if a in range(131,141):a_list.append('N')
        if a in range(141,151):a_list.append('O')

    b_list = []
    for a in list(result_df.artifact_lv_2):
        if a in range(1,11):b_list.append('A')
        if a in range(11,21):b_list.append('B')
        if a in range(21,31):b_list.append('C')
        if a in range(31,41):b_list.append('D')
        if a in range(41,51):b_list.append('E')
        if a in range(51,61):b_list.append('F')
        if a in range(61,71):b_list.append('G')
        if a in range(71,81):b_list.append('H')
        if a in range(81,91):b_list.append('I')
        if a in range(91,101):b_list.append('J')
        if a in range(101,111):b_list.append('K')
        if a in range(111,121):b_list.append('L')
        if a in range(121,131):b_list.append('M')
        if a in range(131,141):b_list.append('N')
        if a in range(141,151):b_list.append('O')

    c_list = []
    for a in list(result_df.artifact_lv_3):
        if a in range(1,11):c_list.append('A')
        if a in range(11,21):c_list.append('B')
        if a in range(21,31):c_list.append('C')
        if a in range(31,41):c_list.append('D')
        if a in range(41,51):c_list.append('E')
        if a in range(51,61):c_list.append('F')
        if a in range(61,71):c_list.append('G')
        if a in range(71,81):c_list.append('H')
        if a in range(81,91):c_list.append('I')
        if a in range(91,101):c_list.append('J')
        if a in range(101,111):c_list.append('K')
        if a in range(111,121):c_list.append('L')
        if a in range(121,131):c_list.append('M')
        if a in range(131,141):c_list.append('N')
        if a in range(141,151):c_list.append('O')

    d_list = []
    for a in list(result_df.artifact_lv_4):
        if a in range(1,11):d_list.append('A')
        if a in range(11,21):d_list.append('B')
        if a in range(21,31):d_list.append('C')
        if a in range(31,41):d_list.append('D')
        if a in range(41,51):d_list.append('E')
        if a in range(51,61):d_list.append('F')
        if a in range(61,71):d_list.append('G')
        if a in range(71,81):d_list.append('H')
        if a in range(81,91):d_list.append('I')
        if a in range(91,101):d_list.append('J')
        if a in range(101,111):d_list.append('K')
        if a in range(111,121):d_list.append('L')
        if a in range(121,131):d_list.append('M')
        if a in range(131,141):d_list.append('N')
        if a in range(141,151):d_list.append('O')

    e_list = []
    for a in list(result_df.artifact_lv_5):
        if a in range(1,11):e_list.append('A')
        if a in range(11,21):e_list.append('B')
        if a in range(21,31):e_list.append('C')
        if a in range(31,41):e_list.append('D')
        if a in range(41,51):e_list.append('E')
        if a in range(51,61):e_list.append('F')
        if a in range(61,71):e_list.append('G')
        if a in range(71,81):e_list.append('H')
        if a in range(81,91):e_list.append('I')
        if a in range(91,101):e_list.append('J')
        if a in range(101,111):e_list.append('K')
        if a in range(111,121):e_list.append('L')
        if a in range(121,131):e_list.append('M')
        if a in range(131,141):e_list.append('N')
        if a in range(141,151):e_list.append('O')

    f_list = []
    for a in list(result_df.artifact_lv_6):
        if a in range(1,11):f_list.append('A')
        if a in range(11,21):f_list.append('B')
        if a in range(21,31):f_list.append('C')
        if a in range(31,41):f_list.append('D')
        if a in range(41,51):f_list.append('E')
        if a in range(51,61):f_list.append('F')
        if a in range(61,71):f_list.append('G')
        if a in range(71,81):f_list.append('H')
        if a in range(81,91):f_list.append('I')
        if a in range(91,101):f_list.append('J')
        if a in range(101,111):f_list.append('K')
        if a in range(111,121):f_list.append('L')
        if a in range(121,131):f_list.append('M')
        if a in range(131,141):f_list.append('N')
        if a in range(141,151):f_list.append('O')

    result_df['a']=pd.DataFrame(a_list)
    result_df['b']=pd.DataFrame(b_list)
    result_df['c']=pd.DataFrame(c_list)
    result_df['d']=pd.DataFrame(d_list)
    result_df['e']=pd.DataFrame(e_list)
    result_df['f']=pd.DataFrame(f_list)

    result_df = result_df.fillna('A')



    return result_df
if __name__ == '__main__':
    settings_dev.set_env('sanguo_ks')
    start_ds = ''
    end_ds = '20170626'
    start_time = '2017-04-13 00:00:00'
    user_info_df = artifact_lv_data(start_ds,end_ds,start_time)
    user_info_df.to_excel('/home/kaiqigu/桌面/机甲无双-金山版-神器数据_20170627-2.xlsx', index=False)
    print "end"

