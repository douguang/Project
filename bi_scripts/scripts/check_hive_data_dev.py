#!/usr/local/bin/python
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
import settings_dev
from settings_dev import hive_template

date_str = sys.argv[1]
date_str2 = datetime.datetime.strptime(date_str, "%Y%m%d").strftime("%Y-%m-%d")

cmd_ip = "/sbin/ifconfig | grep 'inet addr' | awk '{print $2}' | awk -F':' '{print $2}'"
ip = commands.getoutput(cmd_ip)
hadoop_slave5 = "192.168.1.27"
hadoop_slave2 = "192.168.1.47"
hadoop_slave3 = "192.168.1.41"

if hadoop_slave5 in ip:
    a = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime("%Y%m%d%H%M")
    b = date_str + "1700"
    c = datetime.datetime.strptime(b, "%Y%m%d%H%M").strftime("%Y%m%d%H%M")
    if a > c:
        dbs = ['dancer_pub', 'dancer_tx', 'dancer_tw', 'dancer_cgwx', 'superhero_bi', 'superhero_qiku', 'superhero_vt', 'dancer_mul', 'dancer_cgame', 'jianniang_tw', 'jianniang_pub', 'jianniang_bt', 'superhero2_tw', 'dancer_kr','slg_mul', 'crime_empire_pub']
    else:
        dbs = ['dancer_pub', 'dancer_tx', 'dancer_tw', 'dancer_cgwx', 'superhero_bi', 'superhero_qiku', 'superhero_vt', 'dancer_mul', 'dancer_cgame',  'jianniang_tw', 'jianniang_pub', 'jianniang_bt', 'superhero2_tw', 'dancer_kr','slg_mul', 'crime_empire_pub']
elif hadoop_slave2 in ip:
    dbs = ['qiling_ks', 'qiling_tx', 'qiling_ios']
elif hadoop_slave3 in ip:
    dbs = ['sanguo_ks', 'sanguo_tt', 'sanguo_tx', 'sanguo_tw', 'sanguo_tl', 'sanguo_guandu', 'sanguo_chaov', 'metal_pub']
else:
    exit()

for db in dbs:
    settings_dev.set_env(db)
    hdfs_client = InsecureClient(settings_dev.hdfs_url)
    engine = sqlalchemy.create_engine(hive_template.format(db=''))
    conn_hive = engine.raw_connection()

    try:
        cur = conn_hive.cursor()
        for table in settings_dev.raw_table_path.keys():
            local_path = settings_dev.raw_table_path[table].format(date=date_str)
            if db in ['dancer_pub', 'dancer_tx', 'dancer_tw', 'dancer_mul', 'dancer_cgame', 'dancer_cgwx',  'superhero2_tw', 'jianniang_tw', 'jianniang_pub', 'jianniang_bt', 'slg_mul' , 'dancer_kr',] and table.startswith("raw_action"):
                table = table.replace('raw_action', 'parse_action')
                filename = 'parse_' + os.path.basename(local_path)
                local_path = os.path.join(os.path.dirname(local_path), filename)
            elif db in ['dancer_pub', 'dancer_tx', 'dancer_tw', 'dancer_mul', 'dancer_cgwx', 'dancer_cgame',  'superhero2_tw', 'slg_mul', 'dancer_kr', 'superhero2'] and table == "raw_info":
                table = "parse_info"
                filename = 'parse_' + os.path.basename(local_path)
                local_path = os.path.join(os.path.dirname(local_path), filename)
            elif db in ['superhero2_tw', 'superhero2'] and table == "raw_hero":
                table = "parse_hero"
                filename = 'parse_' + os.path.basename(local_path)
                local_path = os.path.join(os.path.dirname(local_path), filename)
            elif db in ['superhero2_tw', 'superhero2'] and table == "raw_equip":
                table = "parse_equip"
                filename = 'parse_' + os.path.basename(local_path)
                local_path = os.path.join(os.path.dirname(local_path), filename)
            elif db in ['superhero2_tw', 'superhero2'] and table == "raw_item":
                table = "parse_item"
                filename = 'parse_' + os.path.basename(local_path)
                local_path = os.path.join(os.path.dirname(local_path), filename)
            elif db in ['superhero2_tw', 'superhero2'] and table == "raw_stones":
                table = "parse_stones"
                filename = 'parse_' + os.path.basename(local_path)
                local_path = os.path.join(os.path.dirname(local_path), filename)
            elif db in ['superhero2_tw', 'superhero2'] and table == "raw_new_user":
                table = "parse_new_user"
                filename = 'parse_' + os.path.basename(local_path)
                local_path = os.path.join(os.path.dirname(local_path), filename)
            elif db in ['superhero2_tw', 'superhero2'] and table == "raw_act_user":
                table = "parse_act_user"
                filename = 'parse_' + os.path.basename(local_path)
                local_path = os.path.join(os.path.dirname(local_path), filename)
            elif db == 'jianniang_pub' and table == "raw_accesslog":
                table = "parse_accesslog"
                filename = 'parse_' + os.path.basename(local_path)
                local_path = os.path.join(os.path.dirname(local_path), filename)
            elif db == 'jianniang_bt' and table == "raw_accesslog":
                table = "parse_accesslog"
                filename = 'parse_' + os.path.basename(local_path)
                local_path = os.path.join(os.path.dirname(local_path), filename)
            elif db == 'slg_mul' and table == "raw_alliance":
                table = "parse_alliance"
                filename = 'parse_' + os.path.basename(local_path)
                local_path = os.path.join(os.path.dirname(local_path), filename)
            elif db == 'slg_mul' and table == "raw_card":
                table = "parse_card"
                filename = 'parse_' + os.path.basename(local_path)
                local_path = os.path.join(os.path.dirname(local_path), filename)
            elif db == 'slg_mul' and table == "raw_city":
                table = "parse_city"
                filename = 'parse_' + os.path.basename(local_path)
                local_path = os.path.join(os.path.dirname(local_path), filename)
            elif db == 'slg_mul' and table == "raw_daily_data":
                table = "parse_daily_data"
                filename = 'parse_' + os.path.basename(local_path)
                local_path = os.path.join(os.path.dirname(local_path), filename)
            elif db == 'slg_mul' and table == "raw_item":
                table = "parse_item"
                filename = 'parse_' + os.path.basename(local_path)
                local_path = os.path.join(os.path.dirname(local_path), filename)
            elif db == 'slg_mul' and table == "raw_skill":
                table = "parse_skill"
                filename = 'parse_' + os.path.basename(local_path)
                local_path = os.path.join(os.path.dirname(local_path), filename)
            elif table == "raw_nginx":
                table = "parse_nginx"
                filename = 'parse_' + os.path.basename(local_path)
                local_path = os.path.join(os.path.dirname(local_path), filename)

            elif db == 'crime_empire_pub' and table == "raw_alliance":
                table = "parse_alliance"
                filename = 'parse_' + os.path.basename(local_path)
                local_path = os.path.join(os.path.dirname(local_path), filename)
            elif db == 'crime_empire_pub' and table == "raw_card":
                table = "parse_card"
                filename = 'parse_' + os.path.basename(local_path)
                local_path = os.path.join(os.path.dirname(local_path), filename)
            elif db == 'crime_empire_pub' and table == "raw_city":
                table = "parse_city"
                filename = 'parse_' + os.path.basename(local_path)
                local_path = os.path.join(os.path.dirname(local_path), filename)
            elif db == 'crime_empire_pub' and table == "raw_daily_data":
                table = "parse_daily_data"
                filename = 'parse_' + os.path.basename(local_path)
                local_path = os.path.join(os.path.dirname(local_path), filename)
            elif db == 'crime_empire_pub' and table == "raw_item":
                table = "parse_item"
                filename = 'parse_' + os.path.basename(local_path)
                local_path = os.path.join(os.path.dirname(local_path), filename)
            elif db == 'crime_empire_pub' and table == "raw_skill":
                table = "parse_skill"
                filename = 'parse_' + os.path.basename(local_path)

            else:
                filename = os.path.basename(local_path)
            if db == 'SLG_test':
                db = 'slg_test'
            hive_path = '/user/hive/warehouse/{db}.db/{table}/ds={date_str}/{filename}'.format(
                db=db,
                table=table,
                filename=filename,
                date_str=date_str)
            if db == 'slg_test':
                db = 'SLG_test'
            if hdfs_client.status(hive_path, strict=False):
                length_hdfs = hdfs_client.status(hive_path, strict=False).get('length', '0')
                if not os.path.exists(local_path):
                    print '{0}:Warning {1} may be deleted'.format(db, local_path)
                else:
                    cmd = "ls -l %s | awk '{print $5}'" % local_path
                    length_local = commands.getoutput(cmd)
                    if int(length_hdfs) != int(length_local):
                        print '{0}:Warning {1} size unequal'.format(db, table)
            else:
                if not os.path.exists(local_path):
                    print '{0}:Average {1} not in hive or local!'.format(db, filename)
                else:
                    print '{0}:Average {1} not in hive!'.format(db, filename)

        for task_name in settings_dev.job_deps:
            if os.path.exists("/home/data/bi_scripts/history_task_test/%s/%s" % (db, date_str2)):
                if task_name not in os.listdir("/home/data/bi_scripts/history_task_test/%s/%s" % (db, date_str2)):
                    print "{0}:Task {1} failed".format(db, task_name)
            else:
                print "{0}:{1} not run".format(db, db)
    finally:
        conn_hive.close()

