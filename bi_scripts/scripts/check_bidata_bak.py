#!/usr/bin/env python
# -*- coding: utf8 -*-

import settings_dev
import datetime
import commands
import sys
import os
import sqlalchemy


hadoop_slave5 = "192.168.1.27"
hadoop_slave3 = "192.168.1.41"

try:
    date_str = sys.argv[1]
except:
    date_str = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime("%Y%m%d")
date_str2 = datetime.datetime.strptime(date_str, "%Y%m%d").strftime("%Y-%m-%d")

cmd_ip = "/sbin/ifconfig | grep 'inet addr' | awk '{print $2}' | awk -F':' '{print $2}'"
ip = commands.getoutput(cmd_ip)

if hadoop_slave5 in ip:
    server_name1 = hadoop_slave5
    a = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime("%Y%m%d%H%M")
    b = date_str + "1500"
    c = datetime.datetime.strptime(b, "%Y%m%d%H%M").strftime("%Y%m%d%H%M")
    if a > c:
        projects = ['dancer_pub', 'dancer_tx', 'dancer_tw', 'dancer_mul', 'dancer_bt', 'dancer_cgwx', 'dancer_cgame', 'dancer_kr', 'jianniang_tw', 'jianniang_pub',
                    'jianniang_bt', 'superhero_bi', 'superhero_qiku', 'superhero_vt', 'superhero2_tw', 'superhero_mul', 'superhero2', 'crime_empire_pub']
    else:
        projects = ['dancer_pub', 'dancer_tx', 'dancer_tw', 'dancer_mul', 'dancer_bt', 'dancer_cgwx', 'dancer_cgame', 'dancer_kr', 'jianniang_tw', 'jianniang_pub',
                    'jianniang_bt', 'superhero_bi', 'superhero_qiku', 'superhero_vt', 'superhero2_tw', 'superhero2', 'crime_empire_pub']
elif hadoop_slave3 in ip:
    server_name1 = hadoop_slave3
    projects = ['sanguo_ks', 'sanguo_tx', 'sanguo_tw', 'sanguo_tl', 'sanguo_guandu', 'sanguo_chaov', 'metal_beta',
                'sanguo_tt', 'sanguo_bt', 'metal_cgame', 'sanguo_xq']
else:
    exit()

for project in projects:
    settings_dev.set_env(project)
    mysql_engine = sqlalchemy.create_engine('mysql+pymysql://devops:kaiqigu@192.168.1.21/devops?charset=utf8')
    con = mysql_engine.connect()

    sql1 = 'select id from devops_project where project_en = "%s"' % project
    project_id = con.execute(sql1)
    project_id = project_id.first()[0]

    for file in settings_dev.raw_table_path.keys():
        local_path = settings_dev.raw_table_path[file].format(date=date_str)
        if os.path.exists(local_path):
            file_name = os.path.basename(local_path)[0:-9]
            cmd = "ls -lh %s | awk '{print $5}'" % local_path
            size = commands.getoutput(cmd)

            date = date_str2
            sql2 = 'select * from devops_backupbi where project_id = "%s" and file_name = "%s" and date = "%s"' % (
                project_id, file_name, date)
            sql3 = 'insert into devops_backupbi (project_id, file_name, size, date) values ("%s", "%s", "%s", "%s" )' % (
                project_id, file_name, size, date)
            sql4 = 'update devops_backupbi set project_id="%s", file_name="%s", size="%s", date="%s" \
                WHERE project_id = "%s" and file_name = "%s" and date = "%s"' % (
                project_id, file_name, size, date, project_id, file_name, date)

            result = con.execute(sql2)

            if result.first():
                con.execute(sql4)
            else:
                con.execute(sql3)
