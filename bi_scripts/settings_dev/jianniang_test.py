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
url = 'http://106.3.133.59:3200/admin/index'
# 代码目录
code_dir = 'jianniang'
# 开服时间
start_date = datetime.date(2017, 5, 23)
# 27上数据的根目录
local_data_dir = '/home/data/jianniang_test'

# 原始表在27上的路径
raw_table_path = {
    'raw_actionlog': '/home/data/jianniang_test/action_log/action_log_{date}',
    'raw_info': '/home/data/jianniang_test/redis_stats/info_{date}',
    'raw_card': '/home/data/jianniang_test/redis_stats/card_{date}',
    'raw_item': '/home/data/jianniang_test/redis_stats/item_{date}',
    'raw_guide': '/home/data/jianniang_test/redis_stats/guide_{date}',
    'raw_task_guide': '/home/data/jianniang_test/redis_stats/task_guide_{date}',
    'raw_sword_rank': '/home/data/jianniang_test/redis_stats/sword_rank_{date}',
    'raw_develop_rank': '/home/data/jianniang_test/redis_stats/develop_rank_{date}',
    'raw_raid': '/home/data/jianniang_test/redis_stats/raid_{date}',
    'raw_nginx': '/home/data/jianniang_test/nginx_log/bi_point.log-{date}',
    'raw_paylog': '/home/data/jianniang_test/paylog/paylog_{date}',
    'raw_spendlog': '/home/data/jianniang_test/spendlog/spendlog_{date}',
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
    'raw_develop_rank': [],
    'raw_raid': [],
    'raw_paylog': [],
    'raw_spendlog': [],
    'mid_info_all': [],
    # 'dis_daily_data': ['raw_info'],
    # 'dis_keep_rate': ['raw_info'],
    # 'dis_coin_acqure_api': ['parse_actionlog'],
    # 'dis_coin_spend_api': ['parse_actionlog'],
    # 'dis_day_coin_spend': ['parse_actionlog'],
}
