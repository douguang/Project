#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Zhang Yongchen
Description : SLG_test
'''
import datetime
# 下载配置的url
url = 'http://cn.wn.hi365.com/dancer_jinshan'
# 代码目录
code_dir = 'SLG'

# 版本
code = 'SLG_test'

start_date = datetime.date(2017, 9, 14)

local_data_dir = '/home/data/SLG_test/'
remote_data_dir = 'admin@120.92.21.179:/data/'
raw_table_path = {
    'raw_nginx': '/home/data/SLG_test/nginx_log/slg_web.{date}.log',
    'raw_actionlog': '/home/data/SLG_test/action_log/game.{date}.log',
    'raw_info': '/home/data/SLG_test/redis_stats/player.{date}.log',
    'raw_alliance': '/home/data/SLG_test/redis_stats/alliance.{date}.log',
    'raw_card': '/home/data/SLG_test/redis_stats/card_bag.{date}.log',
    'raw_item': '/home/data/SLG_test/redis_stats/item_bag.{date}.log',
    'raw_city': '/home/data/SLG_test/redis_stats/player_city.{date}.log',
    'raw_skill': '/home/data/SLG_test/redis_stats/skill_bag.{date}.log',
    'raw_daily_data': '/home/data/SLG_test/redis_stats/daily_info.{date}.log',
    # 'raw_paylog': '/home/data/SLG_test/paylog/paylog_{date}',
    # 'raw_spendlog': '/home/data/SLG_test/spendlog/spendlog_{date}',
    # 'raw_association': '/home/data/SLG_test/all_association_info/all_association_{date}',
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
    'parse_nginx': [],
#     'raw_actionlog': [],
#     'raw_paylog': [],
#     'raw_spendlog': [],
#     'raw_association': [],
#     'mart_assist': ['parse_info', 'raw_paylog', 'raw_spendlog', 'parse_actionlog'],
#     'mid_assist': ['mart_assist'],
    'mid_info': ['parse_info'],
    'mid_info_all': ['parse_info', 'mid_info'],
#     'mid_new_account': ['parse_info', 'mid_info_all'],
#     'dis_daily_data': ['parse_actionlog', 'parse_info', 'raw_paylog'],  # 日常数据
#     # 卡牌装备 - 卡牌进阶 卡牌飞升 装备进阶
#     'dis_card': ['parse_info'],
#     # 每日钻石消耗
#     'dis_day_coin_spend': ['parse_actionlog', 'raw_paylog', 'parse_info'],
#     'dis_keep_rate': ['parse_info'],  # 留存率
#     'dis_pay_detail': ['raw_paylog', 'parse_info'],  # 充值详情
#     'dis_pay_detail_level': ['raw_paylog'],  # 充值档次分布
#     'dis_reg_user_ltv': ['mid_info_all', 'raw_paylog'],  # 注册用户ltv
#     'dis_platform_ltv': ['dis_reg_user_ltv', 'parse_actionlog'],  # 注册用户分渠道ltv
#     'dis_spend_detail': ['raw_spendlog'],  # 分接口钻石消耗
#     'dis_daily_combat_rank': ['mid_info_all', 'raw_paylog'],  # 每日战力排行
#     'dis_coins_rest_rank': ['raw_paylog', 'parse_info'],  # 钻石存量排行
#     # 钻石消费排行
#     'dis_coins_spend_rank': ['raw_spendlog', 'raw_paylog', 'parse_info'],
#     'dis_history_pay_rank': ['mid_info_all', 'raw_paylog'],  # 历史充值排行
#     # 注册用户次日流失等级分布
#     'dis_loss_reg_user_level_dst': ['mid_info_all', 'parse_info'],
#     'dis_vip_level_dst': ['mid_info_all', 'raw_paylog'],  # vip等级分布
#     'dis_merge_server_info': ['raw_paylog', 'parse_info'],  # 合服所需数据
#     # 用户付费情况表
#     'dis_user_pay_detail': ['parse_info', 'raw_paylog', 'mid_info_all'],
#     # 'dis_vip_level_detail': ['mid_info_all', 'raw_paylog'],
#     # 'dis_ip_country_ltv': ['mid_info_all', 'raw_paylog'],  # 地域国家LTV
#     # 'dis_nginx_user_exchange': ['raw_nginx', 'mid_info_all'],  # 用户转化率
#     # 'dis_nginx_device_exchange': ['raw_nginx', 'mid_info_all'],  # 设备转化率
#     'dis_caution_coin_detail': ['parse_actionlog'],  # 钻石预警脚本
#     # 活动
#     # 'dis_activity_group_buy': ['parse_actionlog', ],
#     'dis_activity_limit_hero': ['parse_info', 'parse_actionlog', 'raw_paylog', 'raw_spendlog'],
#     'dis_activity_roulette': ['parse_info', 'parse_actionlog', 'raw_paylog', 'raw_spendlog'],
#     'dis_activity_super_active': ['parse_info', 'raw_paylog', 'raw_spendlog'],
}
