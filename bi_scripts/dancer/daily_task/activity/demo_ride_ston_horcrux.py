#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
'''

import calendar
import pandas as pd
import xlwt
import settings_dev
from utils import ds_add, hql_to_df, update_mysql, hqls_to_dfs, date_range, get_config
import json
import sys

reload(sys)
sys.setdefaultencoding("utf-8")

# 坐骑
def data_reduce():
    info_sql = '''
        select user_id, vip, ride from parse_info where ds = '20180423' and vip>9
    '''

    print info_sql
    info_df = hql_to_df(info_sql)

    def super_types_lines():
        for _, row in info_df.iterrows():
            # print row
            ride = eval(row.ride)
            ride_level_tmp = ride.get('ride')
            if ride_level_tmp is None:
                ride_level = 0
            else:
                ride_level = ride_level_tmp.get('level')
            ston = ride.get('stone_pos')
            print ston
            if ston == None or ston == []:
                ston_1 = 0
                ston_2 = 0
                ston_3 = 0
                ston_4 = 0
                ston_5 = 0
                ston_6 = 0
                ston_7 = 0
                ston_8 = 0
            else:
                ston_1 = ston[0]
                ston_2 = ston[1]
                ston_3 = ston[2]
                ston_4 = ston[3]
                ston_5 = ston[4]
                ston_6 = ston[5]
                ston_7 = ston[6]
                ston_8 = ston[7]
            yield [row.user_id, row.vip, ride_level, ston_1, ston_2, ston_3, ston_4, ston_5, ston_6, ston_7, ston_8]

    user_info_df = pd.DataFrame(super_types_lines(), columns=[
        'user_id', 'vip','ride_level', 'ston_1', 'ston_2', 'ston_3', 'ston_4', 'ston_5', 'ston_6', 'ston_7', 'ston_8'])
    user_info_df.to_excel('/Users/kaiqigu/Desktop/BI_excel/20180424/demo2.xlsx')


# 武魂
def data_reduce_horcrux():
    info_sql = '''
        select user_id, vip, horcrux from parse_info where ds = '20180423' and vip>9
    '''

    print info_sql
    info_df = hql_to_df(info_sql)

    def super_types_lines():
        horcruxes_box = get_config('horcruxes_box')
        for _, row in info_df.iterrows():
            # print row
            horcrux_tmp = eval(row.horcrux)
            horcrux_ = horcrux_tmp.values()
            # print horcrux_
            for key in horcrux_:
                c_id = key.get('c_id')
                level = key.get('level')
                pos = key.get('pos')

                if pos == -1:
                    c_id = 0
                    level = 0
                    quality = 0
                else:
                    print '===='
                    # print dict(horcruxes_box).get(str(c_id), {})
                    # print dict(horcruxes_box).get(str(c_id), {}).get('quality')
                    quality = dict(horcruxes_box).get(str(c_id), {}).get('quality')
                    yield [row.user_id, row.vip, c_id, level, quality]



    user_info_df = pd.DataFrame(super_types_lines(), columns=[
        'user_id', 'vip', 'c_id','level', 'quality'])
    user_info_df.to_excel('/Users/kaiqigu/Desktop/BI_excel/20180424/demo2.xlsx')



if __name__ == '__main__':

    for platform in ['dancer_pub']:
        settings_dev.set_env(platform)
        data_reduce_horcrux()
    print "end"