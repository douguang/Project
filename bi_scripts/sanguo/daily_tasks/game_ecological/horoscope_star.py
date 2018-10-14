#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
@Time    : 17-6-15 下午5:30
@Author  : Andy 
@File    : horoscope_star.py
@Software: PyCharm
Description :   将星数据
'''
import settings_dev
import pandas as pd
from utils import hql_to_df,ds_add, get_config, date_range
from pandas import DataFrame


def horoscope_star():
    user_info_sql = '''
        select  ds,user_id,vip,star_dict from mid_info_all where ds='20170618' and act_time>='2017-04-13 00:00:00'  and star_dict != '{}' group by ds,user_id,vip,star_dict
    '''
    user_info_df = hql_to_df(user_info_sql)
    print user_info_df

    ds_list, user_id_list, vip_list, star_id_list, quality_list = [], [], [], [], []
    for i in range(len(user_info_df)):
        try:
            date = user_info_df.iloc[i, 0]
            user_id = user_info_df.iloc[i, 1]
            vip = user_info_df.iloc[i, 2]
            star_dict = user_info_df.iloc[i, 3]
            star_dict = eval(star_dict)
            for star in star_dict.values():
                c_id = star['c_id']
                quality = star['quality']
                print c_id
                print quality
                ds_list.append(date)
                user_id_list.append(user_id)
                vip_list.append(vip)
                star_id_list.append(c_id)
                quality_list.append(quality)


        except:
            pass

    result_df = pd.DataFrame({'ds': ds_list,
                              'user_id': user_id_list,
                              'vip': vip_list,
                              'star_id': star_id_list,
                              'quality': quality_list,})

    result_df.to_excel('/home/kaiqigu/桌面/机甲无双-泰国-将星.xlsx', index=False)

    # result_df.groupby(['user_id', 'vip']).agg({'star_id': lambda g: g.nunique()}).reset_index().rename(
    #     columns={
    #         'account': 'reg_user_num'
    #     })
    # result_df.to_excel('/home/kaiqigu/桌面/机甲无双-金山-将星.xlsx', index=False)
if __name__ == '__main__':
    settings_dev.set_env('sanguo_tt')
    horoscope_star()
    print "end"