#!/usr/bin/env python
# -*- coding: utf-8 -*-
import sqlalchemy
from hdfs import InsecureClient
import os
import settings
from settings import hive_template
import time
import glob

glob_list = (
    # dancer_tx
    ('/home/data/dancer_tw/spendlog/spendlog_????????', 'raw_spendlog', 'dancer_tw'),
)

def all_load_hdfs_data_to_hive(glob_list):
    for file_pattern, table, db in glob_list:
        for local_path in glob.glob(file_pattern):
            yield (local_path, table, db)

def load_files_to_hive(file_table_db_par_list, check=True):
    hdfs_client = InsecureClient(settings.hdfs_url)
    print 'hdfs_client'
    engine = sqlalchemy.create_engine(hive_template.format(db=''))
    print 'engine'
    conn_hive = engine.raw_connection()
    print 'conn_hive'
    try:
        cur = conn_hive.cursor()
        print 'cur'
        for i in file_table_db_par_list:
            if len(i) == 3:
                local_path, table, db = i
                partition = local_path[-8:]
            elif len(i) == 4:
                local_path, table, db, partition = i
                partition = partition or local_path[-8:]
            else:
                raise Exception('argument %s format is wrong!' % i)

            # print local_path, table, db, partition
            filename = os.path.basename(local_path)
            hdfs_path = '/tmp/{db}/{filename}'.format(**{
                'filename': filename,
                'db': db
            })
            if check:
                hive_path = '/user/hive/warehouse/{db}.db/{table}/ds={partition}/{filename}'.format(
                    db=db,
                    table=table,
                    filename=filename,
                    partition=partition)
                if hdfs_client.status(hive_path, strict=False):
                    # print '{local_path} already in hive!'.format(
                    #     local_path=local_path)
                    continue
            remote_path = hdfs_client.upload(hdfs_path,
                                             local_path,
                                             overwrite=True)

            hql = '''
            load data inpath '{remote_path}'
            into table {db}.{table}
            partition (ds='{partition}')
            '''.format(**{
                'partition': partition,
                'remote_path': remote_path,
                'table': table,
                'db': db,
            })
            print hql
            cur.execute(hql)
            print 'Success: {local_path} -> {db}.{table} partition: {partition}\n'.format(
                **{
                    'table': table,
                    'db': db,
                    'local_path': local_path,
                    'partition': partition
                })
    finally:
        print 'def finally'
        conn_hive.close()

if __name__ == '__main__':
    num = 1
    while num <= 100:
        try:
            load_files_to_hive(all_load_hdfs_data_to_hive(glob_list))
            print 'success, time sleep 3 seconds'
            time.sleep(3)
        except Exception as e:
            print e
        finally:
            num += 1
            print num
