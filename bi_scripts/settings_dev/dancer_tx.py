#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Dong Junshuang
Description : dancer_tx
'''
import datetime

# 下载配置的url
url = 'http://cn.wn.hi365.com/dancer_jinshan'
# 代码目录
code_dir = 'dancer'

# 版本
code = 'dancer_tx'

start_date = datetime.date(2016, 11, 07)

local_data_dir = '/home/data/dancer_tx/'
remote_data_dir = 'admin@123.206.51.215:/data/'
raw_table_path = {
    'raw_nginx': '/home/data/dancer_tx/nginx_log/access.log_{date}',
    'raw_actionlog': '/home/data/dancer_tx/action_log/action_log_{date}',
    'raw_info': '/home/data/dancer_tx/redis_stats/info_{date}',
    'raw_paylog': '/home/data/dancer_tx/paylog/paylog_{date}',
    'raw_spendlog': '/home/data/dancer_tx/spendlog/spendlog_{date}',
    'raw_association':
    '/home/data/dancer_tx/all_association_info/all_association_{date}',
}

job_deps = {
    'parse_actionlog': [],
    'parse_info': [],
    'parse_nginx': [],
    'raw_actionlog': [],
    'raw_paylog': [],
    'raw_spendlog': [],
    'raw_association': [],
    # 'mart_assist':
    # ['parse_info', 'raw_paylog', 'raw_spendlog', 'parse_actionlog'],
    'mid_info_all': ['parse_info'],
    'mid_new_account': ['parse_info', 'mid_info_all'],
}

# 不依赖前一天的hive数据的mid表
independent_list = ['']
