#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Dong Junshuang
Description : 超级英雄 通用sql语句
Time        : 2017.04.07
'''
# 每日快照，用户身上的信息
info_sql = '''
SELECT uid as user_id,
       nick AS name,
       reverse(substr(reverse(uid), 8)) AS server,
       platform_2 AS platform,
       substr(uid,1,1) as plat,
       account,
       level,
       vip_level as vip,
       create_time AS reg_time,
       fresh_time AS act_time,
       zuanshi as coin
FROM raw_info
WHERE ds ='{date}'
'''
# 当日用户充值金额,排除测试用户
pay_sql = '''
SELECT uid as user_id,
       sum(order_money) AS order_money,
       sum(order_coin) AS order_coin
FROM raw_paylog
WHERE ds ='{date}'
  AND platform_2 <> 'admin_test'
GROUP BY uid
'''
# 当日用户消费钻石
spend_sql = '''
SELECT uid as user_id,
       sum(coin_num) AS spend_coin
FROM raw_spendlog
WHERE ds ='{date}'
GROUP BY uid
'''
# 测试用户的数据
gs_sql = '''
SELECT DISTINCT uid AS user_id
FROM mid_gs
'''
# 注册用户的数据
reg_sql = '''
SELECT uid AS user_id,
       substr(uid,1,1) AS plat
FROM raw_reg
WHERE ds ='{date}'
'''
# 卡牌数据
card_sql = '''
SELECT uid as user_id,
       card_id,
       is_fight,
       jinjie,
       zhuansheng
FROM raw_card
WHERE ds = '{date}'
'''
# 卡牌超进化数据
super_sql = '''
SELECT uid as user_id,
       card_id,
       substr(uid,1,1) AS plat,
       super_step_level
FROM raw_super_step
WHERE ds = '{date}'
'''
# bi和自营的活跃用户
bi_act_sql = '''
SELECT ds,
       uid AS user_id,
       substr(uid,1,1) AS plat,
       platform_2 AS platform
FROM raw_reg
WHERE ds >= '{start_date}'
AND ds <= '{end_date}'
'''
# 其他版本活跃用户
act_sql = '''
SELECT ds,
       uid AS user_id,
       substr(uid,1,1) AS plat
FROM raw_reg
WHERE ds >= '{start_date}'
AND ds <= '{end_date}'
'''
