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
    try:
        if 'method=new_user' in line:
            account = line.split('account=')[1].split('&')[0].strip()
            ip = line.split(' - -')[0].strip()
            time = line.split('[')[1].split(']')[0].strip()
            platform_channel = line.split('&platform_channel=')[1].split('&')[0].strip()

            parsed_line = '\t'.join(map(str,[ip, time, account, platform_channel])) + '\n'
            return parsed_line
    except:
        pass
