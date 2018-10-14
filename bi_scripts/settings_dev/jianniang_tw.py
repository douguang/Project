#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
@Time    : 20170111
@Author  : Dong Junshuang
@Software: Sublime Text
Description : 剑娘的配置文件
'''
import datetime

# 下载配置的url
url = 'http://118.193.20.3:3200/admin/index/'
# 代码目录
code_dir = 'jianniang'
# 开服时间
start_date = datetime.date(2018, 7, 17)
# 27上数据的根目录
local_data_dir = '/home/data/jianniang_tw'

# 原始表在27上的路径
raw_table_path = {
    'raw_actionlog': '/home/data/jianniang_tw/action_log/action_log_{date}',
    'raw_info': '/home/data/jianniang_tw/redis_stats/info_{date}',
    'raw_card': '/home/data/jianniang_tw/redis_stats/card_{date}',
    'raw_item': '/home/data/jianniang_tw/redis_stats/item_{date}',
    'raw_guide': '/home/data/jianniang_tw/redis_stats/guide_{date}',
    'raw_task_guide': '/home/data/jianniang_tw/redis_stats/task_guide_{date}',
    'raw_sword_rank': '/home/data/jianniang_tw/redis_stats/sword_rank_{date}',
    'raw_nginx': '/home/data/jianniang_tw/nginx_log/bi_point.log-{date}',
    'raw_paylog': '/home/data/jianniang_tw/paylog/paylog_{date}',
    'raw_spendlog': '/home/data/jianniang_tw/spendlog/spendlog_{date}',
}

# 脚本依赖关系
job_deps = {
    'parse_actionlog': [],
    'parse_nginx': [],
    'raw_info': [],
    'raw_card': [],
    'raw_item': [],
    'raw_guide': [],
    'raw_task_guide': [],
    'raw_sword_rank': [],
    'raw_paylog': [],
    'raw_spendlog': [],
    'mid_info_all': ['raw_info'],
    # 'dis_daily_data': ['raw_info', 'raw_paylog'],
    # 'dis_keep_rate': ['raw_info', 'mid_info_all'],
    # 'dis_coin_acqure_api': ['parse_actionlog'],
    # 'dis_coin_spend_api': ['parse_actionlog'],
    # 'dis_day_coin_spend': ['parse_actionlog'],
    # 'dis_reg_user_ltv': ['mid_info_all', 'raw_info', 'raw_paylog'],
    # 'dis_platform_ltv': ['mid_info_all', 'raw_info', 'dis_reg_user_ltv', 'raw_paylog'],
    # 'dis_card': ['raw_info', 'raw_card'],
    # 'dis_item': ['raw_info', 'raw_item'],
}
