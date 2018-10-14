#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Dong Junshuang
Description : 可同时(检测/删除/导入)同一个游戏的所有(原始表/或多个表/多个日期)的数据
Time        : 2017.04.01
'''
import sqlalchemy
import os
import settings_dev
from hdfs import InsecureClient
from settings_dev import hive_template
from settings_dev import hive_path
from settings_dev import hdfs_url
from utils import date_range
from lib.utils import ColorPrint
from lib.utils import load_data_to_file


def load_to_hive(platform, start_date, end_date, task_name_list=None):
    '''批量导入数据到hive,可同时导入同一个游戏的所有原始表(或多个表)，多个日期的数据
    '''
    settings_dev.set_env(platform)
    task_name_list = task_name_list or settings_dev.raw_table_path.keys()
    for task_name in task_name_list:
        for date in date_range(start_date, end_date):
            message = '>>>start run {0} {1}'.format(platform, task_name)
            ColorPrint(message)
            local_path = settings_dev.raw_table_path[task_name].format(
                date=date)
            filename = os.path.basename(local_path)
            hive_url_path = hive_path.format(db=platform,
                                             table=task_name,
                                             date_str=date,
                                             filename=filename)
            load_data_to_file(hive_url_path, local_path, platform,
                              task_name, date)


def deal_hive_data(hdfs_client, hive_path, is_del):
    '''检测或删除数据
    '''
    try:
        if hdfs_client.status(hive_path, strict=False):
            if is_del:
                try:
                    hdfs_client.delete(hive_path)
                    print '{hive_path} delete Complete'.format(
                        hive_path=hive_path)
                except Exception, e:
                    print e
                    print '{hive_path} delete Faild'.format(
                        hive_path=hive_path)
        else:
            print 'Warning: {hive_path} not in hive!'.format(
                hive_path=hive_path)
    except Exception, e:
        print e


def del_raw_data(db, date, is_del=False, table_list=None):
    '''
    可同时检测或删除同一个游戏的所有原始表(或多个表)，多个日期的数据
    is_del=False表示只检测hive中的数据是否存在, is_del=True表示检测并删除hive中存在的数据
    如果未指定table_list，则默认处理所有的原始表
    '''
    settings_dev.set_env(db)
    table_list = table_list or settings_dev.raw_table_path
    hdfs_client = InsecureClient(hdfs_url)
    engine = sqlalchemy.create_engine(hive_template.format(db=''))
    conn_hive = engine.raw_connection()
    try:
        for table in table_list:
            filename = os.path.basename(settings_dev.raw_table_path[
                table].format(date=date))
            hive_path_str = hive_path.format(db=db,
                                             table=table,
                                             filename=filename,
                                             date_str=date)
            deal_hive_data(hdfs_client, hive_path_str, is_del)
    finally:
        conn_hive.close()


if __name__ == '__main__':
    platform = 'superhero_tw'
    date = '20170331'
    # del_raw_data(platform, date, True)
    for platform in ['superhero_tw']:
        load_to_hive(platform, date, date)
    # del_raw_data(platform, date, True, ['raw_info'])
    # load_to_hive(platform, date, date, ['raw_info'])
