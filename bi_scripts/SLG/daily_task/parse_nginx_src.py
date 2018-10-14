#!/usr/bin/env python  
# encoding: utf-8  

""" 
@author: Andy 
@site:  
@software: PyCharm 
@file: parse_nginx_src.py 
@time: 18/1/21 下午7:12 
"""
#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Dong Junshuang
Description :
'''
import json
from utils import timestamp_datetime


def parse_actionlog(line):
    '''将json格式的动作日志转化为 tab分割 格式
    >>> parse_actionlog('{"body": {"a_rst":  [{"diff": "10", "after": "8692", "obj": "Dmp:FreeMoney", "before": "8682"}, {"diff": "10000", "after": "2192855", "obj": "food", "before": "2182855"}, {"diff": "10", "after": "8692", "obj": "coin", "before": "8682"}, {"diff": "2", "after": "78", "obj": "Item@13", "before": "76"}], "a_tar": {"city_id": "243"}, "a_typ": "country_war_1.get_city_preview", "a_usr": "m120012186@m12"}, "combat": 328464, "food": 17435, "app_id": "", "app_ver": "", "coin_free": 254, "coin": 254, "device_id": "0", "account": "vivo_996eab0de3e46d5a", "level": 33, "coin_charge": 0, "log_t": "1462354217", "vip_level": 0}')
    0
    '''

    l = json.loads(line)
    a_usr = l['body']['a_usr']
    account = l.get('account', '')
    lt = l.get('lt', '')
    a_typ = l['body']['a_typ']
    a_tar = l['body']['a_tar']
    platform = l.get('platform', '')
    appid = l.get('appid', '')
    a_rst = l['body']['a_rst']
    return_code = l['body']['return_code']
    parsed_line = '\t'.join(map(str, [a_usr, account, platform, appid, lt, a_typ, a_tar, a_rst, return_code])) + '\n'
    return parsed_line


def parse_info(line):
    '''将json格式的redis快照info转化为 tab分割 格式
    >>>
    '''

    l = json.loads(line)
    uid = l['data']['common_data_'].get('uid_', '')                     # uid
    account = l['data']['common_data_'].get('account_name_', '')        # 账号
    platform = l['data']['common_data_'].get('platform_', '')           # 渠道
    device_mask = l['data']['common_data_']['device_'].get('device_mask_', '')          # 设备号
    device_name = l['data']['common_data_']['device_'].get('device_name_', '')          # 设备名
    name = l['data']['common_data_'].get('name_', '')                   # 名字
    vip = l['data']['common_data_'].get('vip_', '')                     # vip
    level = l['data'].get('level_', '')                 # 等级
    alliance_id = l['data'].get('alliance_id_', '')                     # 联盟id
    reg_time = l['data'].get('reg_time_', '')                           # 注册时间
    anti_obj = l['data'].get('anti_', '')                                   # 资源反物质
    power = l['data'].get('power_', '')                                 # 势力值
    npc_wall_tiles = l['data'].get('npc_wall_tiles_', '')               # 玩家npc城皮集合
    domain_id = l['data'].get('domain_id_', '')                         # 所在州id
    tiles = l['data'].get('tiles_', '')                                 # 玩家地块集合
    sb_pass_id = l['data'].get('sb_pass_id_', '')                       # 模拟战役关卡id
    fame = l['data'].get('fame_', '')                                   # 声望
    sb_chapter_id = l['data'].get('sb_chapter_id_', '')                 # 模拟战役章节id
    alliance_contribute = l['data'].get('alliance_contribute_', '')     # 联盟贡献
    gas_speed = l['data'].get('gas_speed_', '')                        # 资源气体产量
    metal = l['data'].get('metal_', '')                                 # 资源铁
    sub_city_num = l['data'].get('sub_city_num_', '')                   # 分城个数
    fame_level = l['data'].get('fame_level_', '')                       # 声望等级
    fort_num = l['data'].get('fort_num_', '')                           # 要塞个数
    gas = l['data'].get('gas_', '')                                     # 资源气体
    alliance_name = l['data'].get('alliance_name_', '')                 # 联盟名字
    offline_time = l['data'].get('offline_time_', '')                   # 下线时间
    stamina = l['data'].get('stamina_', '')                             # 体力值
    guide_nodes = l['data'].get('guide_nodes_', '')                     # 玩家新手引导的阶段
    alliance_post = l['data'].get('alliance_post_', '')                 # 所在联盟官职
    metal_speed = l['data'].get('metal_speed_', '')                     # 资源铁产量
    exp = l['data'].get('exp_', '')                                     # 经验
    anti_speed = l['data'].get('anti_speed_', '')                       # 资源反物质产量
    cur_main_quest = l['data'].get('cur_main_quest_', '')
    time = l['time']                                                    # time
    log_id = l['log_id']                                                # log_id
    diamond = l['data'].get('diamond_', '')
    ping = l['data'].get('ping_', '')
    money = l['data'].get('money_', '')
    app_id =  l['data']['common_data_'].get('app_id_', '')

    parsed_line = '\t'.join(map(str, [uid, account, device_mask, device_name, name, vip, level, exp, fame_level, fame, reg_time, metal, gas, anti_obj, metal_speed, gas_speed, anti_speed, alliance_id, alliance_name, domain_id, alliance_post, alliance_contribute,
                                      power, stamina, offline_time, sub_city_num, fort_num, sb_chapter_id, sb_pass_id, tiles, npc_wall_tiles, guide_nodes, cur_main_quest, time, log_id, platform,diamond,ping,money,app_id])) + '\n'
    return parsed_line


def parse_card(line):
    '''将json格式的redis快照hero转化为 tab分割 格式
    >>>
    '''

    l = json.loads(line)
    data = l['data']
    uid = l['data']['common_data_'].get('uid_', '')                     # uid
    account = l['data']['common_data_'].get('account_name_', '')        # 账号
    platform = l['data']['common_data_'].get('platform_', '')           # 渠道
    device_mask = l['data']['common_data_']['device_'].get('device_mask_', '')          # 设备号
    device_name = l['data']['common_data_']['device_'].get('device_name_', '')          # 设备名
    name = l['data']['common_data_'].get('name_', '')                   # 名字
    vip = l['data']['common_data_'].get('vip_', '')                     # vip
    level = l['data']['common_data_'].get('player_level_', '')                 # 等级
    parsed_line = []
    for card in data.keys():
        if card != 'common_data_':
            awaken_lv = data[card].get('awaken_lv_', '')                              # 觉醒等级
            card_level = data[card].get('level_', '')                                      # 卡牌等级
            star = data[card].get('star_', '')                                        # 卡牌星级
            card_id = data[card].get('id_', '')                                       # 卡牌id
            home_tile = data[card].get('home_tile_', '')
            cur_tile = data[card].get('cur_tile_', '')
            unit_num = data[card].get('unit_num_', '')                                # 卡牌兵力
            skill = data[card].get('skill_', '')                                      # 卡牌技能
            time = l['time']                                                    # time
            log_id = l['log_id']                                                # log_id
            parsed_line_one = '\t'.join(map(str, [uid, account, device_mask, device_name, name, vip, level, card_id, card_level, unit_num, star, awaken_lv, skill, home_tile, cur_tile, time, log_id, platform])) + '\n'
            parsed_line.append(parsed_line_one)

    return parsed_line


def parse_alliance(line):
    '''将json格式的redis快照equip转化为 tab分割 格式
    >>>
    '''

    l = json.loads(line)
    power = l['data'].get('power_', '')                                 # 联盟势力值
    member_num = l['data'].get('member_num_', '')                       # 联盟成员数据
    technos = l['data'].get('technos_', '')                             # 联盟科技
    create_time = l['data'].get('create_time_', '')                     # 联盟创建时间
    alliance_id = l['data'].get('id_', '')                              # 联盟id
    gate_lv_num = l['data'].get('gate_lv_num_', '')                     # 占领关卡数
    domain_id = l['data'].get('domain_id_', '')                         # 联盟所在州id
    city_lv_num = l['data'].get('city_lv_num_', '')                     # 占领npc城数
    name = l['data'].get('name_', '')                                   # 联盟名字
    time = l['time']                                                    # time
    log_id = l['log_id']                                                # log_id

    parsed_line = '\t'.join(map(str, [alliance_id, name, power, create_time, member_num, domain_id, city_lv_num, gate_lv_num, technos, time, log_id])) + '\n'
    return parsed_line

def parse_skill(line):
    '''将json格式的redis快照item转化为 tab分割 格式
    >>>
    '''
    l = json.loads(line)
    data = l['data']
    uid = l['data']['common_data_'].get('uid_', '')                     # uid
    account = l['data']['common_data_'].get('account_name_', '')        # 账号
    platform = l['data']['common_data_'].get('platform_', '')           # 渠道
    device_mask = l['data']['common_data_']['device_'].get('device_mask_', '')          # 设备号
    device_name = l['data']['common_data_']['device_'].get('device_name_', '')          # 设备名
    name = l['data']['common_data_'].get('name_', '')                   # 名字
    vip = l['data']['common_data_'].get('vip_', '')                     # vip
    level = l['data']['common_data_'].get('player_level_', '')                 # 等级
    parsed_line = []
    for skill in data.keys():
        if skill != 'common_data_':
            star = data[skill]['star_']                                 # 技能星级
            skill_id = data[skill]['id_']                               # 技能id
            time = l['time']
            log_id = l['log_id']
            parsed_line_one = '\t'.join(map(str, [uid, account, device_mask, device_name, name, vip, level, skill_id, star, time, log_id, platform])) + '\n'
            parsed_line.append(parsed_line_one)
    return parsed_line

def parse_item(line):
    '''将json格式的redis快照item转化为 tab分割 格式
    >>>
    '''
    l = json.loads(line)
    data = l['data']
    uid = l['data']['common_data_'].get('uid_', '')                     # uid
    account = l['data']['common_data_'].get('account_name_', '')        # 账号
    platform = l['data']['common_data_'].get('platform_', '')           # 渠道
    device_mask = l['data']['common_data_']['device_'].get('device_mask_', '')          # 设备号
    device_name = l['data']['common_data_']['device_'].get('device_name_', '')          # 设备名
    name = l['data']['common_data_'].get('name_', '')                   # 名字
    vip = l['data']['common_data_'].get('vip_', '')                     # vip
    level = l['data']['common_data_'].get('player_level_', '')                 # 等级
    parsed_line = []
    for item in data.keys():
        if item != 'common_data_':
            count = data[item]['count_']                                     # 物品数量
            item_id = data[item]['id_']                                      # 物品id
            time = l['time']
            log_id = l['log_id']
            parsed_line_one = '\t'.join(map(str, [uid, account, device_mask, device_name, name, vip, level, item_id, count, time, log_id, platform])) + '\n'
            parsed_line.append(parsed_line_one)
    return parsed_line


def parse_city(line):
    '''将json格式的redis快照stone转化为 tab分割 格式
    >>>
    '''

    l = json.loads(line)
    data = l['data']
    uid = l['data']['common_data_'].get('uid_', '')                     # uid
    account = l['data']['common_data_'].get('account_name_', '')        # 账号
    platform = l['data']['common_data_'].get('platform_', '')           # 渠道
    device_mask = l['data']['common_data_']['device_'].get('device_mask_', '')          # 设备号
    device_name = l['data']['common_data_']['device_'].get('device_name_', '')          # 设备名
    name = l['data']['common_data_'].get('name_', '')                   # 名字
    vip = l['data']['common_data_'].get('vip_', '')                     # vip
    level = l['data']['common_data_'].get('player_level_', '')                 # 等级
    parsed_line = []
    for city in data.keys():
        if city != 'common_data_':
            tile = data[city]['tile_']
            for city in data[city]['builds_'].keys():
                builds_level = data['1']['builds_'][city]['level_']                                     # 建筑等级
                builds_id = data['1']['builds_'][city]['id_']                                    # 建筑id
                time = l['time']
                log_id = l['log_id']
                parsed_line_one = '\t'.join(map(str, [uid, account, device_mask, device_name, name, vip, level, tile, builds_id, builds_level, time, log_id, platform])) + '\n'
                parsed_line.append(parsed_line_one)
    return parsed_line


def parse_daily_data(line):
    '''将json格式的redis快照stone转化为 tab分割 格式
    >>>
    '''
    l = json.loads(line)
    online_num = l['data'].get('online_num', '')
    server_id = l['data'].get('server_id', '')
    charge_player_num = l['data'].get('charge_player_num', '')
    all_player_num = l['data'].get('all_player_num', '')
    cur_charge = l['data'].get('cur_charge', '')
    start_time = l['data'].get('start_time', '')
    today_login = l['data'].get('today_login', '')
    daily_regist_num = l['data'].get('daily_regist_num', '')
    time = l['time']
    log_id = l['log_id']
    parsed_line = '\t'.join(map(str, [online_num, server_id, charge_player_num, all_player_num, cur_charge, start_time, today_login, daily_regist_num, time, log_id])) + '\n'
    return parsed_line

def parse_nginx(line):
    '''解析Nginx日志，提取部分字段，并转化为 tab分割 格式
    >>>
    '''
    ip = line.split(' -')[0].strip()
    time = line.split('[')[1].split(' +')[0].strip()
    gmt = line.split('+')[1].split(']')[0].strip()
    get_post = line.split('"')[1].split(' /')[0].strip()
    api_type = line.split('/')[3].split('/?')[0].strip()

    if 'initLog=' in line:
        initLog = line.split('initLog=')[1].split('&')[0].strip()
    else:
        initLog = ''
    if 'recordType=' in line:
        recordType = line.split('recordType=')[1].split('&')[0].strip()
    else:
        recordType = ''
    if 'eventId=' in line:
        eventId = line.split('eventId=')[1].split('&')[0].strip()
    else:
        eventId = ''
    if 'channel=' in line:
        channel = line.split('channel=')[1].split('&')[0].strip()
    else:
        channel = ''
    if 'deviceMac=' in line:
        deviceMac = line.split('deviceMac=')[1].split('&')[0].strip()
    else:
        deviceMac = ''
    if 'deviceName=' in line:
        deviceName = line.split('deviceName=')[1].split('&')[0].strip()
    else:
        deviceName = ''
    if 'deviceVersion=' in line:
        deviceVersion = line.split('deviceVersion=')[1].split('&')[0].strip()
    else:
        deviceVersion = ''
    if 'deviceTime=' in line:
        deviceTime = timestamp_datetime(int(line.split('deviceTime=')[1].split('&')[0].strip())/1000)
    else:
        deviceTime = ''
    if 'deviceNet=' in line:
        deviceNet = line.split('deviceNet=')[1].split('&')[0].strip()
    else:
        deviceNet = ''
    parsed_line = '\t'.join(map(str, [ip, time, gmt, get_post, api_type, initLog, eventId, recordType, channel, deviceMac, deviceName, deviceVersion, deviceTime, deviceNet])) + '\n'
    return parsed_line
