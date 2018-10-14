#!/usr/bin/env python  
# encoding: utf-8  

""" 
@author: Andy 
@site:  黑帮
@software: PyCharm 
@file:
@time: 18/6/12
"""

import datetime
# 下载配置的url
url = 'http://118.193.20.9/tools/admin/index'
# 代码目录
code_dir = 'crime_empire'

# 版本
code = 'crime_empire_pub'

start_date = datetime.date(2018, 07, 30)

local_data_dir = '/home/data/crime_empire_pub/'
# remote_data_dir = 'admin@120.92.21.179:/data/'

raw_table_path = {
    'raw_actionlog': '/home/data/crime_empire_pub/action_log/game.{date}.log',

    'raw_info': '/home/data/crime_empire_pub/redis_stats/player.{date}.log',
    'raw_alliance': '/home/data/crime_empire_pub/redis_stats/alliance.{date}.log',
    'raw_card': '/home/data/crime_empire_pub/redis_stats/card_bag.{date}.log',
    'raw_item': '/home/data/crime_empire_pub/redis_stats/item_bag.{date}.log',
    'raw_city': '/home/data/crime_empire_pub/redis_stats/player_city.{date}.log',
    'raw_skill': '/home/data/crime_empire_pub/redis_stats/skill_bag.{date}.log',
    'raw_daily_data': '/home/data/crime_empire_pub/redis_stats/daily_info.{date}.log',

    'raw_paylog': '/home/data/crime_empire_pub/paylog/paylog_{date}',
    'raw_nginx': '/home/data/crime_empire_pub/nginx_log/access.log_{date}',
    #新增
    'raw_nginx_1': '/home/data/crime_empire_pub/nginx_log/slg_web.log-{date}',

    'raw_spendlog': '/home/data/crime_empire_pub/spendlog/spendlog_{date}',
}

job_deps = {
    'parse_actionlog': [],
    'parse_info': [],
    'parse_card': [],
    'parse_alliance': [],
    'parse_skill': [],
    'parse_item': [],
    'parse_city': [],
    'parse_daily_data': [],
    'raw_paylog': [],

    'raw_spendlog': [],
    'mid_info': ['parse_info'],
    'mid_info_all': ['parse_info', 'mid_info'],
    'parse_nginx': [],
    #新增
    'parse_nginx_1': [],
    # 用户
    'dis_daily_data':['parse_actionlog', 'parse_info', 'raw_paylog'],
    'dis_keep_rate': ['parse_info'],
    'dis_reg_user_ltv': ['mid_info_all', 'raw_paylog']

    # 营收

}
