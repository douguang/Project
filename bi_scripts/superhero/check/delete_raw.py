#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Dong Junshuang
Description : 检查hive中是否存在raw数据，并批量删除对应的原始数据
'''
import sqlalchemy
import os
import settings_dev
from hdfs import InsecureClient
from settings_dev import hive_template
from utils import date_range


def delete_hive_data(db, date_str):
    # print '===== ' + db + '.' + date_str +' ====='
    settings_dev.set_env(db)

    hdfs_client = InsecureClient(settings_dev.hdfs_url)
    engine = sqlalchemy.create_engine(hive_template.format(db=''))
    conn_hive = engine.raw_connection()

    try:
        # cur = conn_hive.cursor()
        dir_path = os.path.join(settings_dev.BASE_ROOT, 'check_data',
                                settings_dev.platform)
        if not os.path.exists(dir_path):
            os.makedirs(dir_path)

        for table in settings_dev.raw_table_path:
            local_path = settings_dev.raw_table_path[table].format(
                date=date_str)
            filename = os.path.basename(local_path)
            hive_path = '/user/hive/warehouse/{db}.db/{table}/ds={date_str}/{filename}'.format(
                db=db, table=table,
                filename=filename,
                date_str=date_str)
            if hdfs_client.status(hive_path, strict=False):
                length = hdfs_client.status(hive_path, strict=False).get(
                    'length', '0')
                # print table, length
                try:
                    hdfs_client.delete(hive_path)
                    print '{hive_path} is delete!'.format(hive_path=hive_path)
                except Exception, e:
                    print 'delete {hive_path} Faild!'.format(hive_path=hive_path)
                    raise
            else:
                print 'Warning {0} not in hive!'.format(table)
    finally:
        conn_hive.close()
        # print 'Check out'
        # print '-------------------------------------------------'


if __name__ == '__main__':
    # db = 'superhero_bi'
    # date = '20170321'
    # check_hive_data(db, date)
    for db in ['superhero_tw']:
        for date in date_range('20170323', '20170323'):
            print db, date
            delete_hive_data(db, date)

