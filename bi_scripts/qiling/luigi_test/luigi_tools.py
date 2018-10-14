#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Dong Junshuang
Description :
'''
import json


def parse_actionlog(line):
    '''将json格式的动作日志转化为 tab分割 格式
    >>> parse_actionlog('{"body": {"a_rst":  [{"diff": "10", "after": "8692", "obj": "Dmp:FreeMoney", "before": "8682"}, {"diff": "10000", "after": "2192855", "obj": "food", "before": "2182855"}, {"diff": "10", "after": "8692", "obj": "coin", "before": "8682"}, {"diff": "2", "after": "78", "obj": "Item@13", "before": "76"}], "a_tar": {"city_id": "243"}, "a_typ": "country_war_1.get_city_preview", "a_usr": "m120012186@m12"}, "combat": 328464, "food": 17435, "app_id": "", "app_ver": "", "coin_free": 254, "coin": 254, "device_id": "0", "account": "vivo_996eab0de3e46d5a", "level": 33, "coin_charge": 0, "log_t": "1462354217", "vip_level": 0}')
    0
    '''
    l = json.loads(line)
    a_usr = l['body']['a_usr']
    user_id_server = a_usr.split('@')
    user_id, server = max(user_id_server), min(user_id_server)  # 兼容后端写反的情况
    account = l.get('account', '')
    coin = l.get('coin', '')
    coin_free = l.get('coin_free', '')
    coin_charge = l.get('coin_charge', '')
    log_t = l.get('log_t', '')
    vip_level = l.get('vip_level', '')
    level = l.get('level', '')
    combat = l.get('combat', '')
    food = l.get('food', '')
    a_typ = l['body']['a_typ']
    a_tar = l['body']['a_tar']
    return_code = a_tar.pop('return_code', '')
    FreeMoney_before = FreeMoney_after = FreeMoney_diff = ''
    Money_before = Money_after = Money_diff = ''
    obj_without_money = []
    for obj in l['body'].get('a_rst', []):
        if obj['obj'] == 'Dmp:FreeMoney':
            FreeMoney_before = obj['before']
            FreeMoney_after = obj['after']
            FreeMoney_diff = obj['diff']
        elif obj['obj'] == 'Dmp:Money':
            Money_before = obj['before']
            Money_after = obj['after']
            Money_diff = obj['diff']
        else:
            obj_without_money.append(obj)
    parsed_line = '\t'.join(map(str, [user_id, server, account, coin, coin_free, coin_charge, log_t, vip_level, level, combat, food, a_typ,
                                      a_tar, FreeMoney_before, FreeMoney_after, FreeMoney_diff, Money_before, Money_after, Money_diff, return_code, obj_without_money])) + '\n'
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
    if '__ts=' in line:
        ts = line.split('__ts=')[1].split('&')[0].strip()
    else:
        ts = ''
    if 'channel_id=' in line and 'channel_id=&' not in line:
        channel_id = line.split('channel_id=')[1].split('&')[0].strip()
    else:
        channel_id = ''
    if 'version' in line:
        version = line.split('version=')[1].split('&')[0].strip()
    else:
        version = ''
    if 'devicename=' in line:
        devicename = line.split('devicename=')[1].split('&')[0].strip()
    else:
        devicename = ''
    if 'platform_channel=' in line:
        platform_channel = line.split('platform_channel=')[
            1].split('&')[0].strip()
    else:
        platform_channel = ''
    if 'identifier=' in line:
        identifier = line.split('identifier=')[1].split('&')[0].strip()
    else:
        identifier = ''
    if 'device_mark=' in line:
        device_mark = line.split('device_mark=')[1].split('&')[0].strip()
    else:
        device_mark = ''
    if 'device_mem=' in line:
        device_mem = line.split('device_mem=')[1].split('&')[0].strip()
    else:
        device_mem = ''
    if 'rn=' in line and 'rn=&' not in line:
        rn = line.split('rn=')[1].split('&')[0].strip()
    else:
        rn = ''
    if 'pt=' in line and 'pt=&' not in line:
        pt = line.split('pt=')[1].split('&')[0].strip()
    else:
        pt = ''
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
    if 'mk=' in line and 'mk=&' not in line:
        mk = line.split('mk=')[1].split('&')[0].strip()
    else:
        mk = ''
    if 'reward_info=' in line and 'reward_info=&' not in line:
        reward_info = line.split('reward_info=')[1].split('&')[0].strip()
    else:
        reward_info = ''
    parsed_line = '\t'.join(map(str, [ip, time, gmt, get_post, api_type, method, user_token, account, guide_team, guide_id, config_name, ts, pt, channel_id, version,
                                      devicename, platform_channel, identifier, rn, device_mark, device_mem, reward_info, mk, retry, use_port, apply_time, apply_ip, net_delay_2, net_delay_1])) + '\n'
    return parsed_line
