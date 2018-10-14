#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Dong Junshuang
Description :
'''
import datetime

# 下载配置的url
# url = 'http://120.92.15.63/gen_release'       # 线上服地址
url = 'http://192.168.1.9/genesis2'             # 开发服地址
# url = 'http://120.132.57.21/g6/'
# 代码目录
code_dir = 'superhero2'
# 唯一标识
code = 'superhero2_tw'

start_date = datetime.date(2017, 9, 9)

local_data_dir = '/home/data/superhero2_tw/'

raw_table_path = {
    'raw_action_log': '/home/data/superhero2_tw/action_log/action_log_{date}',
    'raw_paylog': '/home/data/superhero2_tw/paylog/paylog_{date}',
    'raw_spendlog': '/home/data/superhero2_tw/spendlog/spendlog_{date}',
    'raw_new_user': '/home/data/superhero2_tw/new_user/reg_{date}',
    'raw_act_user': '/home/data/superhero2_tw/active_user/act_{date}',
    'raw_hero': '/home/data/superhero2_tw/redis_stats/hero_{date}',
    'raw_equip': '/home/data/superhero2_tw/redis_stats/equip_{date}',
    'raw_item': '/home/data/superhero2_tw/redis_stats/item_{date}',
    'raw_stones': '/home/data/superhero2_tw/redis_stats/stones_{date}',
    'raw_info': '/home/data/superhero2_tw/redis_stats/info_{date}',
    'raw_nginx': '/home/data/superhero2_tw/nginx_log/access.log_{date}',
    # 'raw_pet': '/home/data/superhero2_tw/redis_stats/all_pet_{date}',
    # 'raw_scores': '/home/data/superhero2_tw/redis_stats/all_scores_{date}',
    # 'raw_vip_info': '/home/data/superhero2_tw/redis_stats/vip_info_{date}',
    # 'raw_super_step': '/home/data/superhero2_tw/redis_stats/card_super_step_{date}',
}

job_deps = {
    'raw_paylog': [],
    'raw_spendlog': [],
    'parse_action_log': [],
    'parse_hero': [],
    'parse_equip': [],
    'parse_item': [],
    'parse_info': [],
    'parse_stones': [],
    'parse_nginx': [],
    'parse_act_user': [],
    'parse_new_user': [],
    # 'mart_assist': [],
    # 'mid_assist': ['mart_assist'],
    'mid_info_all': ['parse_info'],
    'mid_new_account': ['mid_info_all'],
    # 'dis_act_coinnum_info': ['parse_info'],  # 钻石存量异常用户数据(留存率中)
    # 'dis_card_equip_num': ['parse_equip', 'mid_info_all'],  # 命运装备数量
    # 'dis_card_equip_quality': ['parse_equip', 'mid_info_all'],  # 命运装备品质
    # 'dis_card': ['parse_info'],  # 卡牌进阶、升星
    # 'dis_equip_st_lv': ['parse_info'],  # 装备精炼
    # 'dis_card_use_rate': ['parse_info'],  # 卡牌使用率
    # # # 日常数据
    # 'dis_daily_data': ['parse_info', 'raw_paylog'],
    # # # 每日钻石消费
    # 'dis_day_coin_spend': ['mid_info_all', 'raw_spendlog', 'raw_paylog'],   # 每日钻石消费
    # 'dis_keep_rate': ['mid_info_all'],  # 留存率
    # 'dis_coin_save': ['parse_info'],  # 钻石存量
    # 'dis_loss_reg_user_level_dst': ['mid_info_all'],  # 次日流失等级分布
    # 'dis_keep_user_level_dst': ['mid_info_all'],  # 活跃用户等级分布
    # 'dis_spend_detail': ['mid_info_all'],  # 分接口钻石消耗
    # # # 注册用户LTV(account)
    # 'dis_reg_user_ltv': ['parse_info', 'raw_paylog', 'mid_info_all'],
    # # 'dis_spend_detail': ['raw_spendlog', 'parse_info'],  # 消费详情
    # # # vip等级分布
    # 'dis_vip_level_dst': ['raw_paylog', 'parse_info', 'raw_paylog', 'mid_info_all'],
    # # 'dis_zombie_distr': ['mid_info_all'],  # 僵尸用户
    # # 排行榜
    # 'dis_history_pay_rank': ['mid_info_all', 'raw_paylog'],     # 历史充值排行
    # 'dis_coins_spend_rank':
    # ['raw_spendlog', 'raw_paylog', 'parse_info'],               # 钻石消费排行
    # 'dis_coins_rest_rank': ['raw_paylog', 'parse_info'],        # 钻石存量排行
    # 'dis_daily_combat_rank': ['mid_info_all', 'raw_paylog'],    # 每日战力排行
    # # 'dis_card_super': ['parse_info'],  # 卡牌超进化
    # # 'dis_act_user_coin_save': ['raw_spendlog', 'parse_info'],  # 活跃玩家钻石存量
    # # 'dis_pay_ivtl': ['raw_paylog', 'parse_info'],  # 充值档次分布(国内)
    # # # 新服数据(国内)
    # # 'dis_new_server': ['raw_paylog', 'parse_info'],
    # # 'dis_user_pay_detail': ['parse_info', 'raw_paylog'],  # 用户付费情况
    # # 'dis_pay_rate': ['raw_paylog'],  # 付费用户数占比
}
