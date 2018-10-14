#!/usr/bin/env python  
# encoding: utf-8  

""" 
@author: Andy 
@site:  
@software: PyCharm 
@file: fhly.py 
@time: 17/8/23 下午12:20 
"""

import datetime

# 下载配置的url
url = ''
# 代码目录
code_dir = 'fhly'
# 开服时间
start_date = datetime.date(2017, 7, 10)
# 27上数据的根目录
local_data_dir = '/home/data/fhly'

# 原始表在27上的路径
raw_table_path = {
    'raw_login': '/home/data/fhly/login/login_{date}',
    'raw_account': '/home/data/fhly/account/account_{date}',
    'raw_role': '/home/data/fhly/role/role_{date}',
}

# 脚本依赖关系
job_deps = {
    'parse_account': [],
    'parse_login': [],
    'parse_role': [],

}
