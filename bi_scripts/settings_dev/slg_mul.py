#!/usr/bin/env python  
# encoding: utf-8  

""" 
@author: Andy 
@site:  SLG多语言版本
@software: PyCharm 
@file: slg_mul.py 
@time: 18/1/17 下午3:43 
"""

import datetime
# 下载配置的url
url = 'http://38.83.103.146/tools/admin/'
# 代码目录
code_dir = 'SLG'

# 版本
code = 'slg_mul'

start_date = datetime.date(2018, 01, 19)

local_data_dir = '/home/data/slg_mul/'
# remote_data_dir = 'admin@120.92.21.179:/data/'

raw_table_path = {
    'raw_actionlog': '/home/data/slg_mul/action_log/game.{date}.log',
    'raw_info': '/home/data/slg_mul/redis_stats/player.{date}.log',
    'raw_alliance': '/home/data/slg_mul/redis_stats/alliance.{date}.log',
    'raw_card': '/home/data/slg_mul/redis_stats/card_bag.{date}.log',
    'raw_item': '/home/data/slg_mul/redis_stats/item_bag.{date}.log',
    'raw_city': '/home/data/slg_mul/redis_stats/player_city.{date}.log',
    'raw_skill': '/home/data/slg_mul/redis_stats/skill_bag.{date}.log',
    'raw_daily_data': '/home/data/slg_mul/redis_stats/daily_info.{date}.log',
    'raw_paylog': '/home/data/slg_mul/paylog/paylog_{date}',
    'raw_nginx': '/home/data/slg_mul/nginx_log/access.log_{date}',
    # 'raw_spendlog': '/home/data/SLG_test/spendlog/spendlog_{date}',
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
    'mid_info': ['parse_info'],
    'mid_info_all': ['parse_info', 'mid_info'],
    'parse_nginx': [],
    # 用户
    'dis_daily_data':['parse_actionlog', 'parse_info', 'raw_paylog'],
    'dis_keep_rate': ['parse_info'],
    'dis_reg_user_ltv': ['mid_info_all', 'raw_paylog']

    # 营收

}
