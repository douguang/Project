#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Dong Junshuang
Description :
'''
import datetime

# 下载配置的url
# url = 'http://pub.kaiqigu.net/genesisandroidios'
url = 'http://120.132.57.21/g6/'
# 代码目录
code_dir = 'superhero'
# 唯一标识
code = 'superhero_bi'

start_date = datetime.date(2014, 6, 12)

local_data_dir = '/home/data/superhero/'

raw_table_path = {
    'raw_action_log': '/home/data/superhero/action_log/action_log_{date}',
    'raw_paylog': '/home/data/superhero/paylog/paylog_{date}',
    'raw_spendlog': '/home/data/superhero/spendlog/spendlog_{date}',
    'raw_reg': '/home/data/superhero/new_user/reg_{date}',
    'raw_act': '/home/data/superhero/active_user/act_{date}',
    'raw_card': '/home/data/superhero/redis_stats/card_{date}',
    'raw_equip': '/home/data/superhero/redis_stats/equip_{date}',
    'raw_item': '/home/data/superhero/redis_stats/item_{date}',
    'raw_info': '/home/data/superhero/redis_stats/info_{date}',
    'raw_pet': '/home/data/superhero/redis_stats/all_pet_{date}',
    'raw_scores': '/home/data/superhero/redis_stats/all_scores_{date}',
    'raw_vip_info': '/home/data/superhero/redis_stats/vip_info_{date}',
    'raw_super_step':
    '/home/data/superhero/redis_stats/card_super_step_{date}',
    'raw_talisman': '/home/data/superhero/redis_stats/all_talisman_{date}',
    'raw_soul': '/home/data/superhero/redis_stats/all_soul_{date}',
    'raw_nginx': '/home/data/superhero/nginx_log/access.log_{date}',
}

# 任务
job_deps = {
    'raw_action_log': [],
    'raw_paylog': [],
    'raw_spendlog': [],
    'raw_soul': [],
    'raw_reg': [],
    'raw_act': [],
    'raw_card': [],
    'raw_equip': [],
    'raw_item': [],
    'raw_info': [],
    'raw_pet': [],
    'raw_scores': [],
    'raw_vip_info': [],
    'raw_super_step': [],
    'raw_talisman': [],
    # 'mid_paylog_all': ['raw_paylog'],
    # 'mid_gs': ['raw_paylog'], # 后续有新的测试用户由运营提供UID,原因补单也使用admin_test
    'mid_info_all': ['raw_info'],
    'mid_new_account': ['raw_reg', 'raw_info', 'mid_info_all'],
    # 用户中间整合数据
    'mart_assist':
    ['raw_info', 'raw_paylog', 'raw_spendlog', 'raw_reg', 'mart_paylog'],
    'mart_card': ['raw_info', 'raw_card', 'raw_super_step'],  # 卡牌中间整合数据
    'mart_paylog': ['raw_paylog', 'mid_info_all'],  # 所有用户历史充值总额
    'parse_nginx': [],
    # 用户
    'dis_daily_data': ['mart_assist'],  # 日常数据
    'dis_vip_level_dst': ['mart_assist'],  # vip等级分布
    'dis_keep_rate': ['raw_reg', 'raw_info'],  # 留存率
    'dis_new_server': ['mart_assist', 'raw_reg', 'raw_act'],  # 新服数据
    # 卡牌 - 卡牌转生、进阶、超进化
    'dis_card': ['mart_card', 'raw_super_step', 'mart_assist'],
    # 营收
    'dis_day_coin_spend': ['mid_info_all', 'mart_assist'],  # 每日钻石消费
    'dis_spend_detail': ['raw_spendlog'],  # 分接口钻石消费
    'dis_reg_user_ltv': ['raw_paylog', 'mid_new_account'],  # 注册用户LTV(account)
    'dis_platform_ltv': ['parse_info', 'raw_paylog', 'mid_info_all', 'dis_reg_user_ltv'],   # 渠道ltv
    'dis_act_user_coin_save': ['mart_assist'],  # 活跃玩家钻石存量
    'dis_pay_ivtl': ['mart_assist'],  # 充值档次分布
    'dis_user_pay_detail':
    ['mid_new_account', 'mart_assist', 'mart_paylog'],  # 用户付费情况
    'dis_pay_rate': ['mart_assist'],  # 付费用户收入占比
    'dis_pay_platform': ['raw_paylog'],  # 充值的渠道金额统计
    'dis_revenu_buy': ['raw_paylog', 'raw_action_log'],  # 黄金组合拳
    'dis_equip_ltv': ['mid_info_all', 'raw_paylog'],  # 每日分包LTV
    # 活动
    'dis_activity_bowl': ['raw_action_log'],  # 聚宝盆活动
    # 周期报表
    'dis_cycle_data': ['mart_assist'],  # 周期数据
}

# 不依赖前一天的hive数据的mid表
independent_list = ['mid_new_account']

# 依赖前一天的任务
dependent_list = ['mart_paylog']
