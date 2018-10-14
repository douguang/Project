#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
@Time    : 2017/8/4 0004 14:34
@Author  : Andy 
@File    : sanguo_mth.py
@Software: PyCharm
Description :
'''

import datetime

# 下载配置的url
url = ''
# 代码目录
code_dir = 'sanguo'
# 开服时间
start_date = datetime.date(2017, 8,21)
# 27上数据的根目录
local_data_dir = '/home/data/sanguo_mth'

# 原始表在27上的路径
raw_table_path = {
    'raw_nginx': '/home/data/sanguo_mth/nginx_log/access.log_{date}',
    'raw_actionlog': '/home/data/sanguo_mth/action_log/action_log_{date}',
    'raw_activeuser': '/home/data/sanguo_mth/active_user/act_{date}',
    'raw_citynum': '/home/data/sanguo_mth/city_num/city_num_{date}',
    'raw_honorrank': '/home/data/sanguo_mth/honor_rank/honor_rank_{date}',
    'raw_info': '/home/data/sanguo_mth/redis_stats/info_{date}',
    'raw_paylog': '/home/data/sanguo_mth/paylog/paylog_{date}',
    'raw_registeruser': '/home/data/sanguo_mth/new_user/reg_{date}',
    'raw_spendlog': '/home/data/sanguo_mth/spendlog/spendlog_{date}',
    'raw_association': '/home/data/sanguo_mth/redis_stats/all_association_{date}',
    'raw_voided_data': '/home/data/sanguo_mth/voided_orders/voided_data_{date}',
    'raw_sdk_nginx': '/home/data/sanguo_mth/sdk_tw_nginx_log/app.tw.hi365.com.access.log_{date}',
}

# 脚本依赖关系
job_deps = {
    # hive表
    'parse_actionlog': [],
    'raw_actionlog': [],
    'parse_nginx': [],
    'parse_voided_data': [],
    'parse_sdk_nginx': [],
    'raw_activeuser': [],
    'raw_citynum': [],
    'raw_honorrank': [],
    'raw_paylog': [],
    'raw_registeruser': [],
    'raw_spendlog': [],
    'raw_association': [],
    'raw_info': [],
    'mid_info_all': ['raw_info'],
    'mid_register_account': ['raw_info', 'raw_registeruser'],
    'mid_active_account': ['raw_info', 'raw_activeuser'],

    # # 用户 【日常数据  留存率  流失用户  回流用户】
    # 'dis_act_daily_info': ['mid_info_all', 'raw_paylog', 'raw_registeruser', 'raw_activeuser', 'raw_info'],     # 日常数据
    # 'dis_keep_rate': ['raw_activeuser', 'raw_registeruser'],                                                    # 留存率
    # 'dis_act_loss_info': ['raw_activeuser', 'mid_info_all'],                                                    # 流失用户
    # 'dis_act_reloss_info': ['raw_activeuser', 'mid_info_all'],                                                  # 回流用户
    #
    # # 营收 【分接口钻石消费 分接口钻石新增 每日钻石消费 注册用户LTV 充值用户分布 充值档次分布 钻石消费档次分布 新增大R分布 用户付费详情 】
    # 'dis_spend_api': ['raw_spendlog', 'mid_info_all'],                                                          # 分接口钻石消费
    # 'dis_coin_achieve_api': ['raw_paylog', 'parse_actionlog'],                                                  # 分接口钻石新增
    # 'dis_days_coin_spend': ['parse_actionlog', 'raw_paylog', 'raw_info'],                                       # 每日钻石消费
    # 'dis_reg_user_ltv': ['mid_info_all', 'raw_paylog'],                                                         # 注册用户LTV
    # 'dis_pay_platform': ['mid_info_all', 'raw_activeuser', 'raw_registeruser', 'raw_paylog'],                   # 充值用户分布
    # 'dis_pay_all_level': ['raw_paylog'],                                                                        # 充值档次分布
    # 'dis_spend_all_level': ['raw_spendlog'],                                                                    # 钻石消费档次分布
    # 'dis_user_pay_detail': ['raw_info', 'raw_paylog', 'mid_info_all'],                                          # 用户付费详情
    # 'dis_new_big_r_info': ['mid_info_all', 'raw_spendlog', 'raw_paylog'],                                       # 新增大R分布
    # 'dis_vioded_orders': ['parse_voided_data', 'parse_sdk_nginx',],                                             # 退单数据
}
