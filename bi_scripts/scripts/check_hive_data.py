#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Dong Junshuang
Description : 检查数据是否导入hive
'''
import sqlalchemy
from hdfs import InsecureClient
import os
import commands
import sys
import datetime
import settings
from settings import hive_template

date_str = sys.argv[1]
date_str2 = datetime.datetime.strptime(date_str, "%Y%m%d").strftime("%Y-%m-%d")

cmd_ip = "ifconfig | grep 'inet addr' | awk '{print $2}' | awk -F':' '{print $2}'"
ip = commands.getoutput(cmd_ip)
hadoop_slave5 = "192.168.1.27"
hadoop_slave2 = "192.168.1.47"
hadoop_slave3 = "192.168.1.41"
if hadoop_slave5 in ip:
    a = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime("%Y%m%d%H%M")
    b = date_str + "1500"
    c = datetime.datetime.strptime(b, "%Y%m%d%H%M").strftime("%Y%m%d%H%M")
    if a > c:
        dbs = ['dancer_pub', 'dancer_tx', 'dancer_tw', 'superhero_bi', 'superhero_qiku', 'superhero_vt', 'superhero_tw', 'superhero_self_en']
    else:
        dbs = ['dancer_pub', 'dancer_tx', 'dancer_tw', 'superhero_bi', 'superhero_qiku', 'superhero_vt', 'superhero_tw']
elif hadoop_slave2 in ip:
    dbs = ['qiling_ks', 'qiling_tx', 'qiling_ios']
elif hadoop_slave3 in ip:
    dbs = ['sanguo_ks', 'sanguo_tx', 'sanguo_tw', 'sanguo_tl', 'sanguo_kr', 'sanguo_ios', 'sanguo_in']
else:
    exit()

for db in dbs:
    settings.set_env(db)
    hdfs_client = InsecureClient(settings.hdfs_url)
    engine = sqlalchemy.create_engine(hive_template.format(db=''))
    conn_hive = engine.raw_connection()

    try:
        cur = conn_hive.cursor()

        for table in settings.raw_table_path:
            local_path = settings.raw_table_path[table]['local'].format(date=date_str)
            if db in ['dancer_pub', 'dancer_tx', 'dancer_tw'] and table == "raw_info":
                filename = 'parsed_' + os.path.basename(local_path)
                local_path = os.path.join(os.path.dirname(local_path),filename)
            else:
                filename = os.path.basename(local_path)
            if not os.path.exists(local_path):
                print '{0}:Average {1} is not in local'.format(db,local_path)
            hive_path = '/user/hive/warehouse/{db}.db/{table}/ds={date_str}/{filename}'.format(
                db=db,
                table=table,
                filename=filename,
                date_str=date_str)
            if hdfs_client.status(hive_path, strict=False):
                length_hdfs = hdfs_client.status(hive_path, strict=False).get('length','0')
                cmd = "ls -l %s | awk '{print $5}'" % local_path
                length_local = commands.getoutput(cmd)
                if int(length_hdfs) != int(length_local):
                    print '{0}:Warning {1} size unequal'.format(db,table)
            else:
                print '{0}:Average {1} not in hive!'.format(db,table)

        if os.path.exists("/home/data/bi_scripts/history_task_test/%s/%s % (db, date_str2)"):
            for task_name in settings.job_deps:
                if task_name not in os.listdir("/home/data/bi_scripts/history_task_test/%s/%s" % (db, date_str2)):
                    print "{0}:Task {1} failed".format(db,task_name)
        else:
            print "{0}:{1} not run".format(db, db)

    finally:
        conn_hive.close()

