#!/usr/bin/env python
# -- coding: UTF-8 --
'''
@Time : 2017/11/7 0007 12:23
@Author : Zhang Yongchen
@File : dancer_kr.py.py
@Software: PyCharm Community Edition
Description :
'''

import datetime
# 下载配置的url
url = 'http://enterhw.wn.hi365.com/pub_multi_lan'
# 代码目录
code_dir = 'dancer'
# 版本
code = 'dancer_kr'
start_date = datetime.date(2017, 11, 8)
local_data_dir = '/home/data/dancer_kr/'

raw_table_path = {
    'raw_nginx': '/home/data/dancer_kr/nginx_log/access.log_{date}',
    'raw_info': '/home/data/dancer_kr/redis_stats/info_{date}',
    'raw_paylog': '/home/data/dancer_kr/paylog/paylog_{date}',
    'raw_spendlog': '/home/data/dancer_kr/spendlog/spendlog_{date}',
    'raw_actionlog': '/home/data/dancer_kr/action_log/action_log_{date}',
    'raw_association':
    '/home/data/dancer_kr/all_association_info/all_association_{date}',
    'raw_voided_data':
    '/home/data/dancer_kr/voided_orders/voided_data_{date}',
    'raw_sdk_nginx':
    '/home/data/dancer_kr/sdk_tw_nginx_log/app.tw.hi365.com.access.log_{date}',
}

job_deps = {
    'parse_actionlog': [],
    'parse_info': [],
    'parse_nginx': [],
    'parse_voided_data': [],
    'parse_sdk_nginx': [],
    'raw_paylog': [],
    'raw_spendlog': [],
    'raw_association': [],
    # 'mart_assist':
    # ['parse_info', 'raw_paylog', 'raw_spendlog', 'parse_actionlog'],
    # 'mid_assist': ['mart_assist'],
    'mid_info_all': ['parse_info'],
    'mid_new_account': ['parse_info'],
    # # 用户
    'dis_daily_data_mul': ['parse_info', 'raw_paylog'],  # 日常数据
    'dis_keep_rate_mul': ['mid_info_all'],  # 留存率
    'dis_vip_level_dst': ['mid_info_all', 'raw_paylog'],  # vip等级分布
    'dis_loss_reg_user_level_dst': ['mid_info_all', 'parse_info'],  # 次日流失等级分布
    # 卡牌装备
    'dis_card': ['parse_info'],  # 卡牌进阶 卡牌飞升 装备进阶
    # 营收
    'dis_reg_user_ltv_mul':
    ['mid_new_account', 'mid_info_all', 'raw_paylog'],  # 注册用户ltv
    'dis_reg_user_ltv_lan_mul':
    ['mid_new_account', 'mid_info_all', 'raw_paylog'],  # 注册用户分渠道ltv
    'dis_day_coin_spend':
    ['parse_actionlog', 'raw_paylog', 'parse_info'],  # 每日钻石消耗
    'dis_spend_detail': ['raw_spendlog'],  # 消费详情
    'dis_pay_detail': ['raw_paylog', 'parse_info'],  # 充值类型
    'dis_pay_detail_level': ['raw_paylog'],  # 常规充值
    'dis_merge_server_info': ['raw_paylog', 'parse_info'],  # 合服所需数据
    'dis_user_pay_detail':
    ['parse_info', 'raw_paylog', 'mid_new_account', 'mid_info_all'],  # 用户付费情况
    # 排行榜
    'dis_history_pay_rank': ['mid_info_all', 'raw_paylog'],  # 历史充值排行
    'dis_coins_spend_rank':
    ['raw_spendlog', 'raw_paylog', 'parse_info'],  # 钻石消费排行
    'dis_coins_rest_rank': ['raw_paylog', 'parse_info'],  # 钻石存量排行
    'dis_daily_combat_rank': ['mid_info_all', 'raw_paylog'],  # 每日战力排行
    # 活动数据
    'dis_activity_limit_hero':
    ['parse_info', 'parse_actionlog', 'raw_paylog', 'raw_spendlog'],  # 限时神将
    'dis_activity_roulette':
    ['parse_info', 'parse_actionlog', 'raw_paylog', 'raw_spendlog'],  # 幸运轮盘
    'dis_activity_super_active':
    ['parse_info', 'raw_paylog', 'raw_spendlog'],  # 宇宙最强
    # 预警
    'dis_caution_coin_detail': ['parse_actionlog'],  # 钻石预警脚本
    # 'dis_vip_level_detail': ['mid_info_all', 'raw_paylog'],
    # 'dis_ip_country_ltv': ['mid_info_all', 'raw_paylog'],  # 地域国家LTV
    # 'dis_nginx_user_exchange': ['raw_nginx', 'mid_info_all'],  # 用户转化率
    # 'dis_nginx_device_exchange': ['raw_nginx', 'mid_info_all'],  # 设备转化率
    # 活动
    # 'dis_activity_group_buy': ['parse_actionlog', ],
    # 'dis_vioded_orders': ['parse_voided_data',
    #                       'parse_sdk_nginx', ],
}

# 不依赖前一天的hive数据的mid表
independent_list = ['mid_new_account']
