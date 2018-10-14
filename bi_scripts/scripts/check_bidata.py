#!/usr/bin/env python
# -*- coding: utf8 -*-

import settings_dev
import datetime
import commands
import sys
import os
import sqlalchemy

try:
    date_str = sys.argv[1]
except:
    date_str = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime("%Y%m%d")
date_str2 = datetime.datetime.strptime(date_str, "%Y%m%d").strftime("%Y-%m-%d")

cmd_ip = "/sbin/ifconfig | grep 'inet addr' | awk '{print $2}' | awk -F':' '{print $2}'"
ip = commands.getoutput(cmd_ip)
hadoop_slave5 = "192.168.1.27"
hadoop_slave2 = "192.168.1.47"
hadoop_slave3 = "192.168.1.41"
if hadoop_slave5 in ip:
    server_name1 = hadoop_slave5
    a = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime("%Y%m%d%H%M")
    b = date_str + "1310"
    c = datetime.datetime.strptime(b, "%Y%m%d%H%M").strftime("%Y%m%d%H%M")
    if a > c:
        dbs = ['dancer_pub', 'dancer_tx', 'dancer_tw', 'dancer_mul', 'jianniang_tw', 'jianniang_pub', 'superhero_bi', 'superhero_qiku', 'superhero_vt', 'superhero2_tw', 'superhero_mul', 'dancer_kr', 'dancer_cgame', 'slg_mul', 'dancer_bt']
    else:
        dbs = ['dancer_pub', 'dancer_tx', 'dancer_tw', 'dancer_mul', 'jianniang_tw', 'jianniang_pub', 'superhero_bi', 'superhero_qiku', 'superhero_vt', 'superhero2_tw', 'dancer_kr', 'dancer_cgame', 'dancer_bt']
elif hadoop_slave2 in ip:
    server_name1 = hadoop_slave2
    dbs = ['qiling_ks', 'qiling_tx', 'qiling_ios']
elif hadoop_slave3 in ip:
    server_name1 = hadoop_slave3
    dbs = ['sanguo_ks', 'sanguo_tx', 'sanguo_tw', 'sanguo_tl', 'sanguo_mth', 'sanguo_kr', 'sanguo_ios', 'sanguo_in', 'sanguo_tt', 'sanguo_bt', 'qiling_ks', 'metal_beta']
else:
    exit()

for db in dbs:
    settings_dev.set_env(db)
    mysql_engine = sqlalchemy.create_engine('mysql+pymysql://emos:Kaiqigu1@192.168.1.21/emos?charset=utf8')
    con = mysql_engine.connect()

    project1 = db
    bak_type1 = "local"

    for table in settings_dev.raw_table_path.keys():
        local_path = settings_dev.raw_table_path[table].format(date=date_str)
        if os.path.exists(local_path):
            bak_file_name1 = os.path.basename(local_path)[0:-9]
            cmd = "ls -lh %s | awk '{print $5}'" % local_path
            bak_size1 = commands.getoutput(cmd)
            bak_date1 = date_str2
            sql1 = 'select * from backup_backup_bi where project = "%s" and bak_file_name = "%s" and bak_date = "%s"' % (project1, bak_file_name1, bak_date1)

            sql2 = 'insert into backup_backup_bi (project, bak_type, server_name, bak_file_name, bak_size, bak_date) ' \
                   'values ("%s", "%s", "%s", "%s", "%s", "%s" )' % (project1, bak_type1, server_name1, bak_file_name1, bak_size1, bak_date1)

            sql3 = 'update backup_backup_bi ' \
                   'set bak_type="{bak_type}", server_name="{server_name}", bak_size="{bak_size}", bak_date="{bak_date}" ' \
                   'WHERE project="{project}"  and bak_file_name="{bak_file_name}" and bak_date="{bak_date}"'.format(
                project=project1, bak_type=bak_type1, server_name=server_name1, bak_file_name=bak_file_name1, bak_size=bak_size1, bak_date=bak_date1)
            result = con.execute(sql1)

            if result.first() is None:
                con.execute(sql2)
            else:
                con.execute(sql3)

print "done"

