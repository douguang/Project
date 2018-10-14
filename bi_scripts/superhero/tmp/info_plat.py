#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Dong Junshuang
Description : 修改info的渠道
'''
import pandas as pd
import settings_dev
from utils import hql_to_df
from utils import ds_add
from utils import date_range
from utils import DateFormat

settings_dev.set_env('superhero_tw')

path = '/Users/kaiqigu/Documents/scripts/'
column = ['uid', 'account', 'name', 'platform_2']
columns = ['uid', 'platform_2']
result_columns = ['uid', 'account', 'nick', 'platform_2', 'device',
                  'create_time', 'fresh_time', 'vip_level', 'level',
                  'zhandouli', 'food', 'metal', 'energy', 'nengjing',
                  'zuanshi', 'qiangnengzhichen', 'chaonengzhichen',
                  'xingdongli', 'xingling', 'jinbi', 'lianjingshi', 'shenen',
                  'gaojishenen', 'gaojinengjing', 'jingjichangdianshu']

for date in date_range('20170112', '20170322'):
    print date
    print DateFormat(date)
    f_in = path + 'sup_info/info_' + DateFormat(date)
    f_out = path + 'parse_info/info_' + date

    info_sql = '''
    SELECT *
    FROM raw_info
    WHERE ds ='{date}'
    '''.format(date=date)
    info_df = hql_to_df(info_sql)
    plat_df = pd.read_table(f_in, names=column)[columns]
    del info_df['platform_2']
    result_df = info_df.merge(plat_df,on='uid')
    result_df = result_df[result_columns]
    result_df.to_csv(f_out, sep = '\t', index = False, header = False)




