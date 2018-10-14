#!/usr/bin/env python
# -- coding: UTF-8 --
'''
@File : metal_pub.py
@Software: PyCharm Community Edition
Description :
'''
import datetime

# 下载配置的url
url = 'http://enter.hjzjcn.hi365.com/BumbleBee/'
# 代码目录
code_dir = 'metal'
# 开服时间
start_date = datetime.date(2018, 03, 14)
# 27上数据的根目录
local_data_dir = '/home/data/metal_pub'

# 原始表在27上的路径
raw_table_path = {
    'raw_nginx': '/home/data/metal_beta/nginx_log/access.log_{date}',
    'raw_actionlog': '/home/data/metal_beta/action_log/action_log_{date}',
    'raw_activeuser': '/home/data/metal_beta/active_user/act_{date}',
    'raw_citynum': '/home/data/metal_beta/city_num/city_num_{date}',
    'raw_honorrank': '/home/data/metal_beta/honor_rank/honor_rank_{date}',
    'raw_info': '/home/data/metal_beta/redis_stats/info_{date}',
    'raw_paylog': '/home/data/metal_beta/paylog/paylog_{date}',
    'raw_registeruser': '/home/data/metal_beta/new_user/reg_{date}',
    'raw_spendlog': '/home/data/metal_beta/spendlog/spendlog_{date}',
    'raw_association': '/home/data/metal_beta/redis_stats/all_association_{date}',
}

# 脚本依赖关系
job_deps = {
    'parse_actionlog': [],
    'raw_actionlog': [],
    'parse_nginx': [],
    'raw_activeuser': [],
    'raw_citynum': [],
    'raw_honorrank': [],
    'raw_paylog': [],
    'raw_registeruser': [],
    'raw_spendlog': [],
    'raw_association': [],
    'raw_info': [],
    # 'mart_assist': ['raw_paylog', 'raw_spendlog', 'parse_actionlog', 'raw_info'],
    # 'mid_assist': ['mart_assist'],
    'mid_info_all': ['raw_info'],
    # 'mid_register_account': ['raw_info', 'raw_registeruser'],
    # 'mid_active_account': ['raw_info', 'raw_activeuser'],
    'dis_new_big_r_info': ['mid_info_all', 'raw_spendlog', 'raw_paylog'],
    'dis_gongping_rob': ['raw_actionlog', 'mid_info_all'],
    'dis_pay_platform': ['mid_info_all', 'raw_activeuser', 'raw_registeruser', 'raw_paylog'],
    'dis_pay_all_level': ['raw_paylog'],
    'dis_spend_all_level': ['raw_spendlog'],
    'dis_spend_api': ['raw_spendlog', 'mid_info_all'],
    'dis_coin_achieve_api': ['raw_paylog', 'parse_actionlog'],  # 分接口钻石新增
    'dis_keep_rate': ['raw_activeuser', 'raw_registeruser'],  # 留存率
    'dis_daily_pay_rank': ['raw_paylog', 'mid_info_all'],  # 每日充值排行榜
    'dis_history_pay_rank': ['raw_paylog', 'mid_info_all'],  # 历史充值排行榜
    'dis_coins_rest_rank': ['raw_paylog', 'raw_info'],  # 每日钻石存量排名Top30
    # 每日钻石消耗排名Top30
    'dis_coins_spend_rank': ['raw_spendlog', 'raw_info', 'raw_paylog'],
    'dis_daily_combat_rank': ['raw_info', 'raw_paylog'],  # 每日战斗力排名Top30
    'dis_act_3day_card_career': ['mid_info_all'],  # 卡牌转职
    'dis_act_3day_card_evo': ['mid_info_all'],  # 卡牌进阶
    'dis_act_3day_card_using_rate': ['mid_info_all'],  # 卡牌使用率
    'dis_act_3day_fight_girl': ['mid_info_all'],  # 战姬状态
    'dis_act_3day_star_fight_hiuzong': ['mid_info_all'],  # 将星汇总状态
    'dis_act_3day_star_fight': ['mid_info_all'],  # 单分服将星状态
    # 日常数据
    'dis_act_daily_info': ['mid_info_all', 'raw_paylog', 'raw_registeruser', 'raw_activeuser', 'raw_info'],
    # 'dis_spirit_info': ['mid_info_all', 'parse_actionlog'],  # 魂灵等级分布#######
    # 'dis_common_newpay_rate': ['raw_registeruser', 'raw_paylog'],  # 新增用户付费比例
    'dis_common_loss_user': ['mid_info_all'],  # 流失用户分布
    'dis_common_uid_level': ['raw_registeruser', 'mid_info_all'],   # 新用户等级分布
    # 新用户等级分布详情
    'dis_common_uid_level_detail': ['raw_registeruser', 'mid_info_all'],
    'dis_common_pay_loss': ['mid_info_all'],  # 付费流失用户生命周期
    # 注册用户LTV
    'dis_reg_user_ltv': ['mid_info_all', 'raw_paylog'],
    'dis_first_month_ltv': ['raw_registeruser', 'raw_paylog'],  # 开服前四周LTV
    # 滚服数据
    'dis_act_roll_server_info': ['raw_registeruser', 'raw_paylog', 'mid_info_all', 'raw_info'],
    # 各服务器历史累计充值金额排名Top10
    'dis_history_pay_rank_server': ['raw_paylog', 'mid_info_all'],
    # 各服务器当日充值金额排名Top10
    'dis_daily_pay_rank_server': ['raw_paylog', 'mid_info_all'],
    # 服务器每日钻石消耗排名Top10
    'dis_coins_spend_rank_server': ['raw_spendlog', 'raw_paylog', 'mid_info_all'],
    # 服务器每日钻石存量排名Top10
    'dis_coins_rest_rank_server': ['raw_info', 'raw_paylog', 'mid_info_all'],
    # 服务器每日战力排名Top10
    'dis_daily_combat_rank_server': ['raw_info', 'raw_paylog', 'mid_info_all'],
    # 每日钻石消耗
    'dis_days_coin_spend': ['parse_actionlog', 'raw_paylog', 'raw_info'],
    # 'dis_nginx_user_exchange': ['parse_nginx', 'mid_info_all'],  # 用户转化率
    # 'dis_nginx_device_exchange': ['parse_nginx', 'mid_info_all'],  # 设备转化率
    # 用户付费情况表
    'dis_user_pay_detail': ['raw_info', 'raw_paylog', 'mid_info_all'],
    # 活动
    # dis_vioded_orders 退单数据
}
