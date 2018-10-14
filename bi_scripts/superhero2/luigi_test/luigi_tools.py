#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Dong Junshuang
Description :
'''
import json
from utils import timestamp_datetime


def parse_action_log(line):
    '''将json格式的动作日志转化为 tab分割 格式
    >>> parse_action_log('{"body": {"a_rst":  [{"diff": "10", "after": "8692", "obj": "Dmp:FreeMoney", "before": "8682"}, {"diff": "10000", "after": "2192855", "obj": "food", "before": "2182855"}, {"diff": "10", "after": "8692", "obj": "coin", "before": "8682"}, {"diff": "2", "after": "78", "obj": "Item@13", "before": "76"}], "a_tar": {"city_id": "243"}, "a_typ": "country_war_1.get_city_preview", "a_usr": "m120012186@m12"}, "combat": 328464, "food": 17435, "app_id": "", "app_ver": "", "coin_free": 254, "coin": 254, "device_id": "0", "account": "vivo_996eab0de3e46d5a", "level": 33, "coin_charge": 0, "log_t": "1462354217", "vip_level": 0}')
    0
    '''

    l = json.loads(line)
    a_usr = l['body']['a_usr']
    user_id_server = a_usr.split('@')
    user_id, server = max(user_id_server), min(user_id_server)  # 兼容后端写反的情况
    account = l.get('account', '')
    diamond = l.get('diamond', '')
    log_t = l.get('log_t', '')
    vip_exp = l.get('vip_exp', '')
    level = l.get('level', '')
    coin = l.get('coin', '')
    silver = l.get('silver', '')
    # platform = l.get('platform', '')
    exp = l.get('exp', '')
    device_mac = l.get('device_mac', '')
    a_typ = l['body']['a_typ']
    a_tar = l['body']['a_tar']

    #platform = l.get('account', '').split('_')[0]
    #修改渠道
    #platform = l.get('tpt', '')

    platform = l['body']['a_tar']['tpid'][0]

    # device_mac = a_tar['device_mk'][0]
    a_rst = l['body']['a_rst']
    return_code = l['body']['return_code']
    diamond_free_before = diamond_free_after = diamond_free_diff = ''
    diamond_charge_before = diamond_charge_after = diamond_charge_diff = ''
    obj_without_money = []
    for obj in l['body'].get('a_rst', []):
        if obj['obj'] == 'diamond_free':
            diamond_free_before = obj['before']
            diamond_free_after = obj['after']
            diamond_free_diff = obj['diff']
        elif obj['obj'] == 'diamond_charge':
            diamond_charge_before = obj['before']
            diamond_charge_after = obj['after']
            diamond_charge_diff = obj['diff']
        else:
            obj_without_money.append(obj)
    parsed_line = '\t'.join(map(str, [user_id, server, account, platform, diamond, coin, silver, log_t, vip_exp, level, exp, a_typ, a_tar, a_rst, diamond_free_before, diamond_free_after, diamond_free_diff, diamond_charge_before, diamond_charge_after, diamond_charge_diff, return_code, device_mac])) + '\n'
    return parsed_line


def parse_info(line):
    '''将json格式的redis快照info转化为 tab分割 格式
    >>>
    '''

    l = json.loads(line)
    uuid = l.get('uuid', '')
    account = l.get('account', '')
    user_id = l.get('user_id', '')
    name = l.get('name', '')
    level = l.get('level', '')
    act_time = l.get('act_time', '')
    dark_coin = l.get('dark_coin', '')
    challenge = l.get('challenge', '')
    silver_ticket = l.get('silver_ticket', '')
    reg_time = l.get('reg_time', '')
    guide = l.get('guide', '')
    # platform = l.get('platform', '')
    #修改用户渠道
    #platform = l.get('account', '').split('_')[0]

    platform = l.get('tpid', '')

    vip = l.get('vip', '')
    father_server = l.get('father_server', '')
    server = l.get('server', '')
    combat = l.get('combat', '')
    device_mark = l.get('device_mark', '')
    diamond_free = l.get('diamond_free', '')
    diamond_charge = l.get('diamond_charge', '')
    create_date = l.get('create_date', '')
    coin = l.get('coin', '')
    silver = l.get('silver', '')
    diamond_ticket = l.get('diamond_ticket', '')
    appid = l.get('appid', '')
    ip = l.get('ip', '')
    account_reg = timestamp_datetime(l.get('account_reg', ''))

    parsed_line = '\t'.join(map(str, [uuid, account, user_id, name, level, act_time, dark_coin, challenge, silver_ticket, reg_time, guide, platform, vip, father_server, server, combat, device_mark, diamond_free, diamond_charge, create_date, coin, silver, diamond_ticket, appid, ip, account_reg])) + '\n'
    return parsed_line


def parse_hero(line):
    '''将json格式的redis快照hero转化为 tab分割 格式
    >>>
    '''

    l = json.loads(line)
    skill = l.get('skill', '')
    is_awaken = l.get('is_awaken', '')
    user_id = l.get('user_id', '')
    combat = l.get('combat', '')
    level = l.get('level', '')
    act_time = l.get('act_time', '')
    mag_atk = l.get('mag_atk', '')
    hp = l.get('hp', '')
    evo = l.get('evo', '')
    equip_pos = l.get('equip_pos', '')
    hero_oid = l.get('hero_oid', '')
    create_date = l.get('create_date', '')
    extra_skill = l.get('extra_skill', '')
    hero_aquire_time = l.get('hero_aquire_time', '')
    phy_def = l.get('phy_def', '')
    milestone = l.get('milestone', '')
    star = l.get('star', '')
    mag_def = l.get('mag_def', '')
    phy_atk = l.get('phy_atk', '')
    hero_id = l.get('hero_id', '')

    parsed_line = '\t'.join(map(str, [user_id, combat, level, act_time, mag_atk, hp, evo, equip_pos, hero_oid, is_awaken, create_date, skill, extra_skill, hero_aquire_time, phy_def, milestone, star, mag_def, phy_atk, hero_id])) + '\n'
    return parsed_line


def parse_equip(line):
    '''将json格式的redis快照equip转化为 tab分割 格式
    >>>
    '''

    l = json.loads(line)
    create_date = l.get('create_date', '')
    extra = l.get('extra', '')
    grade = l.get('grade', '')
    refined_property = l.get('refined_property', '')
    base = l.get('base', '')
    owner = l.get('owner', '')
    quality = l.get('quality', '')
    st_lv = l.get('st_lv', '')
    init_lv = l.get('init_lv', '')
    user_id = l.get('user_id', '')
    level = l.get('level', '')
    act_time = l.get('act_time', '')
    equip_id = l.get('equip_id', '')
    aquire_time = l.get('aquire_time', '')
    refine_count = l.get('refine_count', '')

    parsed_line = '\t'.join(map(str, [user_id, create_date, extra, grade, refined_property, base, owner, quality, st_lv, init_lv, level, act_time, equip_id, aquire_time, refine_count])) + '\n'
    return parsed_line


def parse_item(line):
    '''将json格式的redis快照item转化为 tab分割 格式
    >>>
    '''

    l = json.loads(line)
    act_time = l.get('act_time', '')
    item_id = l.get('item_id', '')
    create_date = l.get('create_date', '')
    user_id = l.get('user_id', '')
    item_num = l.get('item_num', '')

    parsed_line = '\t'.join(map(str, [user_id, act_time, item_id, create_date, item_num])) + '\n'
    return parsed_line


def parse_stones(line):
    '''将json格式的redis快照stone转化为 tab分割 格式
    >>>
    '''

    l = json.loads(line)
    act_time = l.get('act_time', '')
    stone_id = l.get('stone_id', '')
    create_date = l.get('create_date', '')
    stone_num = l.get('stone_num', '')
    user_id = l.get('user_id', '')

    parsed_line = '\t'.join(map(str, [user_id, act_time, stone_id, create_date, stone_num])) + '\n'
    return parsed_line


def parse_nginx(line):
    '''解析Nginx日志，提取部分字段，并转化为 tab分割 格式
    >>>
    '''
    ip = line.split(' -')[0].strip()
    time = line.split('[')[1].split(' +')[0].strip()
    gmt = line.split('+')[1].split(']')[0].strip()
    get_post = line.split('"')[1].split(' /')[0].strip()
    api_type = line.split('/')[4].split('/?')[0].strip()
    net_delay_1 = line.split(' ')[-1].strip()
    net_delay_2 = line.split(' ')[-2].strip()
    apply_ip = line.split(' ')[-3].strip()
    apply_time = line.split(' ')[-7].strip()
    use_port = line.split(' ')[-8].strip()
    if 'method=' in line:
        method = line.split('method=')[1].split('&')[0].strip()
    else:
        method = ''
    if 'luaver=' in line:
        luaver = line.split('luaver=')[1].split('&')[0].strip()
    else:
        luaver = ''
    if 'resver=' in line:
        resver = line.split('resver=')[1].split('&')[0].strip()
    else:
        resver = ''
    if '__ts=' in line:
        ts = line.split('__ts=')[1].split('&')[0].strip()
    else:
        ts = ''
    if 'pt=' in line:
        pt = line.split('pt=')[1].split('&')[0].strip()
    else:
        pt = ''
    if 'appver' in line:
        appver = line.split('appver=')[1].split('&')[0].strip()
    else:
        appver = ''
    if 'device=' in line:
        device = line.split('device=')[1].split('&')[0].strip()
    else:
        device = ''
    if 'pt_chl=' in line:
        pt_chl = line.split('pt_chl=')[1].split('&')[0].strip()
    else:
        pt_chl = ''
    if 'os=' in line:
        os = line.split('os=')[1].split('&')[0].strip()
    else:
        os = ''
    if 'osver=' in line:
        osver = line.split('osver=')[1].split('&')[0].strip()
    else:
        osver = ''
    if 'device_mark=' in line:
        device_mark = line.split('device_mark=')[1].split('&')[0].strip()
    else:
        device_mark = ''
    if 'uuid=' in line:
        uuid = line.split('uuid=')[1].split('&')[0].strip()
    else:
        uuid = ''
    if 'device_mem=' in line:
        device_mem = line.split('device_mem=')[1].split('&')[0].strip()
    else:
        device_mem = ''
    if 'sid=' in line:
        sid = line.split('sid=')[1].split('&')[0].strip()
    else:
        sid = ''
    if 'retassid=' in line and 'retassid=&' not in line and 'retassid= ' not in line:
        retassid = line.split('retassid=')[1].split('&')[0].strip()
    else:
        retassid = ''
    if 'config_name=' in line:
        config_name = line.split('config_name=')[
            1].split('&')[0].strip()
    else:
        config_name = ''
    if 'user_token=' in line and 'user_token=&' not in line:
        user_token = line.split('user_token=')[1].split('&')[0].strip()
    else:
        user_token = ''
    if 'guide_team=' in line:
        guide_team = line.split('guide_team=')[1].split('&')[0].strip()
    else:
        guide_team = ''
    if 'guide_id=' in line:
        guide_id = line.split('guide_id=')[1].split('&')[0].strip()
    else:
        guide_id = ''
    if 'account=' in line and 'account=&' not in line:
        account = line.split('account=')[1].split('&')[0].strip()
    else:
        account = ''
    if 'retry=' in line:
        retry = line.split('retry=')[1].split('&')[0].strip()
    else:
        retry = ''
    if 'appid=' in line:
        appid = line.split('appid=')[1].split('&')[0].strip()
    else:
        appid = ''
    parsed_line = '\t'.join(map(str, [ip, time, gmt, get_post, api_type, method, user_token, account, guide_team, guide_id, config_name, appid, luaver, resver, ts, pt, appver, device, pt_chl, os, osver, device_mark, device_mem, sid, retassid, retry, use_port, apply_time, apply_ip, net_delay_2, net_delay_1, uuid])) + '\n'
    return parsed_line


#x新增
def parse_nginx_1(line):
    '''解析Nginx日志，提取部分字段，并转化为 tab分割 格式
    >>>
    '''
    ip = line.split(' -')[0].strip()
    time = line.split('[')[1].split(' +')[0].strip()
    gmt = line.split('+')[1].split(']')[0].strip()
    get_post = line.split('"')[1].split(' /')[0].strip()
    api_type = line.split('/')[4].split('/?')[0].strip()
    net_delay_1 = line.split(' ')[-1].strip()
    net_delay_2 = line.split(' ')[-2].strip()
    apply_ip = line.split(' ')[-3].strip()
    apply_time = line.split(' ')[-7].strip()
    use_port = line.split(' ')[-8].strip()
    if 'method=' in line:
        method = line.split('method=')[1].split('&')[0].strip()
    else:
        method = ''
    if 'luaver=' in line:
        luaver = line.split('luaver=')[1].split('&')[0].strip()
    else:
        luaver = ''
    if 'resver=' in line:
        resver = line.split('resver=')[1].split('&')[0].strip()
    else:
        resver = ''
    if '__ts=' in line:
        ts = line.split('__ts=')[1].split('&')[0].strip()
    else:
        ts = ''
    if 'pt=' in line:
        pt = line.split('pt=')[1].split('&')[0].strip()
    else:
        pt = ''
    if 'appver' in line:
        appver = line.split('appver=')[1].split('&')[0].strip()
    else:
        appver = ''
    if 'device=' in line:
        device = line.split('device=')[1].split('&')[0].strip()
    else:
        device = ''
    if 'pt_chl=' in line:
        pt_chl = line.split('pt_chl=')[1].split('&')[0].strip()
    else:
        pt_chl = ''
    if 'os=' in line:
        os = line.split('os=')[1].split('&')[0].strip()
    else:
        os = ''
    if 'osver=' in line:
        osver = line.split('osver=')[1].split('&')[0].strip()
    else:
        osver = ''
    if 'device_mark=' in line:
        device_mark = line.split('device_mark=')[1].split('&')[0].strip()
    else:
        device_mark = ''
    if 'uuid=' in line:
        uuid = line.split('uuid=')[1].split('&')[0].strip()
    else:
        uuid = ''
    if 'device_mem=' in line:
        device_mem = line.split('device_mem=')[1].split('&')[0].strip()
    else:
        device_mem = ''
    if 'sid=' in line:
        sid = line.split('sid=')[1].split('&')[0].strip()
    else:
        sid = ''
    if 'retassid=' in line and 'retassid=&' not in line and 'retassid= ' not in line:
        retassid = line.split('retassid=')[1].split('&')[0].strip()
    else:
        retassid = ''
    if 'config_name=' in line:
        config_name = line.split('config_name=')[
            1].split('&')[0].strip()
    else:
        config_name = ''
    if 'user_token=' in line and 'user_token=&' not in line:
        user_token = line.split('user_token=')[1].split('&')[0].strip()
    else:
        user_token = ''
    if 'guide_team=' in line:
        guide_team = line.split('guide_team=')[1].split('&')[0].strip()
    else:
        guide_team = ''
    if 'guide_id=' in line:
        guide_id = line.split('guide_id=')[1].split('&')[0].strip()
    else:
        guide_id = ''
    if 'account=' in line and 'account=&' not in line:
        account = line.split('account=')[1].split('&')[0].strip()
    else:
        account = ''
    if 'retry=' in line:
        retry = line.split('retry=')[1].split('&')[0].strip()
    else:
        retry = ''
    if 'appid=' in line:
        appid = line.split('appid=')[1].split('&')[0].strip()
    else:
        appid = ''
    #新增
    if '&id=' in line:
        id = line.split('&id=')[1].split('&')[0].strip()
    else:
        id = ''
    # 新增最后面的  id

    # 新增
    if '&tpid=' in line:
        tpid = line.split('&tpid=')[1].split('&')[0].strip()
    else:
        tpid = ''
    # 新增最后面的  tpid

    parsed_line = '\t'.join(map(str, [ip, time, gmt, get_post, api_type, method, user_token, account, guide_team, guide_id, config_name, appid, luaver, resver, ts, pt, appver, device, pt_chl, os, osver, device_mark, device_mem, sid, retassid, retry, use_port, apply_time, apply_ip, net_delay_2, net_delay_1, uuid, id, tpid])) + '\n'
    return parsed_line



def parse_act_user(line):
    '''每日活跃用户
    >>>
    '''

    l = json.loads(line)
    user_id = l.get('uid', '')
    platform = l.get('plat', '')
    date = l.get('date', '')
    time = l.get('time', '')

    parsed_line = '\t'.join(map(str, [user_id, platform, date, time])) + '\n'
    return parsed_line


def parse_new_user(line):
    '''每日活跃用户
    >>>
    '''

    l = json.loads(line)
    user_id = l.get('uid', '')
    platform = l.get('plat', '')
    date = l.get('date', '')
    time = l.get('time', '')

    parsed_line = '\t'.join(map(str, [user_id, platform, date, time])) + '\n'
    return parsed_line