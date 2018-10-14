#!/usr/bin/python
# -*- coding: UTF-8 -*-
'''
Author      : Dong Junshuang
Description : hive 分区批量重命名(适用于中间表导入hive后，批量将分区中数据的名称改为000000_0)
'''
import os
from hdfs import InsecureClient
client = InsecureClient('http://192.168.1.8:50070')
from pandas import DataFrame

superhero_bi = {'table':['superhero_bi'],
'column':
[
    'raw_action_log',
    'raw_paylog',
    'raw_spendlog',
    'raw_reg',
    'raw_act',
    'raw_ios_hc',
    'raw_mix_hc',
    'raw_card',
    'raw_equip',
    'raw_item',
    'raw_info',
    'raw_pet',
    'raw_scores',
    'raw_vip_info',
    'raw_super_step'
]}
superhero_qiku = {'table':['superhero_qiku'],
'column':
[
    'raw_action_log',
    'raw_paylog',
    'raw_spendlog',
    'raw_reg',
    'raw_act',
    'raw_card',
    'raw_equip',
    'raw_item',
    'raw_info',
    'raw_pet',
    'raw_scores',
    'raw_super_step'
]}
superhero_vt = {'table':['superhero_vt'],
'column':
[
    'raw_action_log',
    'raw_act',
    'raw_active_stats',
    'raw_card',
    'raw_equip',
    'raw_gem',
    'raw_info',
    'raw_item',
    'raw_paylog',
    'raw_pet',
    'raw_reg',
    'raw_soul',
    'raw_spendlog',
    'raw_super_step'
]}
superhero_tl = {'table':['superhero_tl'],
'column':
[
    'raw_action_log',
    'raw_act',
    'raw_card',
    'raw_equip',
    'raw_gem',
    'raw_info',
    'raw_item',
    'raw_paylog',
    'raw_pet',
    'raw_reg',
    'raw_soul',
    'raw_spendlog',
    'raw_super_step'
]}
superhero_en = {'table':['superhero_en'],
'column':
[
    'raw_action_log',
    'raw_act',
    'raw_card',
    'raw_equip',
    'raw_info',
    'raw_item',
    'raw_paylog',
    'raw_reg',
    'raw_spendlog',
    'raw_super_step'
]}
superhero_usa ={'table':['superhero_usa'],
'column':
[
    'raw_action_log',
    'raw_act',
    'raw_card',
    'raw_equip',
    'raw_info',
    'raw_item',
    'raw_paylog',
    'raw_reg',
    'raw_spendlog'
]}

for i in [superhero_bi,superhero_qiku,superhero_vt,superhero_tl,superhero_en]:
# for i in [superhero_usa]:
    plat_name = i['table'][0]
    table_list = i['column']
    a,b = [],[]
    for table in table_list:
        dir_hdfs_path = '/user/hive/warehouse/{plat_name}.db/{table}'.format(plat_name=plat_name,table=table)
        print dir_hdfs_path
        for path, dirs, files in client.walk(dir_hdfs_path):
            a.append(table)
            print path
            b.append(path)
    result_df = DataFrame({'table':a,'messge':b})
    result_df.to_excel('/Users/kaiqigu/Downloads/Excel/{plat_name}_data.xlsx'.format(plat_name=plat_name))
    print '{plat_name} complite!'.format(plat_name=plat_name)


# print list(client.walk(dir_hdfs_path))
# for path, dirs, files in client.walk(dir_hdfs_path):
#     print path
    # , dirs, files
    # for file in files:
    #     file_path = os.path.join(path, file)
    #     rename_path = os.path.join(path, '000000_0')
    #     client.rename(file_path, rename_path)
# print 'reneme complite!'
