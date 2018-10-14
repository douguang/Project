#!/usr/bin/env python
# -- coding: UTF-8 --
'''
@Time : 2017/8/24 0024 17:28
@Author : Zhang Yongchen
@File : jianniang_pub.py
@Software: PyCharm Community Edition
Description :
'''

import datetime

# 下载配置的url
url = 'http://101.251.250.5:3200/admin/index/'
# 代码目录
code_dir = 'jianniang'
# 开服时间
start_date = datetime.date(2018, 6, 7)
# 27上数据的根目录
local_data_dir = '/home/data/jianniang_bt'

# 原始表在27上的路径
raw_table_path = {
    'raw_actionlog': '/home/data/jianniang_bt/action_log/action_log_{date}',
    'raw_info': '/home/data/jianniang_bt/redis_stats/info_{date}',
    'raw_card': '/home/data/jianniang_bt/redis_stats/card_{date}',
    'raw_item': '/home/data/jianniang_bt/redis_stats/item_{date}',
    'raw_guide': '/home/data/jianniang_bt/redis_stats/guide_{date}',
    'raw_task_guide': '/home/data/jianniang_bt/redis_stats/task_guide_{date}',
    'raw_sword_rank': '/home/data/jianniang_bt/redis_stats/sword_rank_{date}',
    'raw_nginx': '/home/data/jianniang_bt/nginx_log/bi_point.log-{date}',
    'raw_accesslog': '/home/data/jianniang_bt/nginx_log/access.log_{date}',
    'raw_paylog': '/home/data/jianniang_bt/paylog/paylog_{date}',
    'raw_spendlog': '/home/data/jianniang_bt/spendlog/spendlog_{date}',
}

# 脚本依赖关系
job_deps = {
    'parse_actionlog': [],
    'parse_nginx': [],
    'parse_accesslog': [],
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
