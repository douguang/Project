#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
@Time    : 2018/1/23 0023 16:00
@Author  : Wang Fuguo
@File    : luigi_tools.py
@Software: PyCharm
Description :
'''

import json
from urllib import unquote

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
    if 'appversion=' in line:
        appversion = line.split('appversion=')[1].split('&')[0].strip()
    else:
        appversion = ''
    parsed_line = '\t'.join(map(str, [ip, time, gmt, get_post, api_type, method, user_token, account, guide_team, guide_id, config_name, ts, pt, channel_id, version,
                                      devicename, platform_channel, identifier, rn, device_mark, device_mem, reward_info, mk, retry, use_port, apply_time, apply_ip, net_delay_2, net_delay_1,appversion])) + '\n'
    return parsed_line

def parse_voided_data(line):
    '''
    将json格式的动作日志转化为 tab分割 格式 Andy
    '''
    l = json.loads(line)
    data_dict = {'kind':'','voidedTimeMillis':'','purchaseToken':'','purchaseTimeMillis':'',}
    data_list = ['purchaseTimeMillis','purchaseToken','voidedTimeMillis','kind']
    if 'kind' in l.keys(): data_dict['kind'] = l.get('kind', '').strip()
    if 'voidedTimeMillis' in l.keys(): data_dict['voidedTimeMillis'] = l.get('voidedTimeMillis', '').strip()
    if 'purchaseToken' in l.keys(): data_dict['purchaseToken'] = l.get('purchaseToken', '').strip()
    if 'purchaseTimeMillis' in l.keys(): data_dict['purchaseTimeMillis'] = l.get('purchaseTimeMillis', '').strip()
    parse_list = [data_dict[key] for key in data_list]
    parsed_line = '\t'.join(map(str, parse_list)) + '\n'
    # print parsed_line
    return parsed_line

def parse_sdk_nginx(line):
    '''解析SDK的Nginx日志，提取部分字段，并转化为 tab分割 格式
        HTTP/1.1" 200 63
    >>>
    '''
    data_dic={'ip':'','time':'','get_post':'','api_type':'','deviceid':'','device':'','osVer':'','os':'','app_id':'','version':'','deviceid1':'','lang':'','fb_id':'','sign':'','session_id':'','uid':'','orderId':'','packageName':'','productId':'','purchaseTime':'','purchaseState':'','developerPayload':'','purchaseToken':'','data_signature':'',}
    data_list = ['ip','time','get_post','api_type','deviceid','device','osVer','os','app_id','version','deviceid1','lang','fb_id','sign','session_id','uid','orderId','packageName','productId','purchaseTime','purchaseState','developerPayload','purchaseToken','data_signature',]
    data = line.split('"')[1].split('"')[0].strip()
    if data != '' and data.find('http://') == -1 and data.find('\\x') == -1 and data.find('CONNECT') == -1:
        data_dic['ip'] = line.split(' -')[0].strip()
        data_dic['time'] = line.split('[')[1].split(' +')[0].strip()
        data_dic['get_post'] = line.split('"')[1].split(' /')[0].strip()
        if data.find('/?') != -1:
            data_dic['api_type'] = data.split(' /')[1].split('/?')[0].strip()
            result = data.split('/?')[1].split('HTTP/1.1')[0].strip()
            if 'deviceid=' in result: data_dic['deviceid'] = result.split('deviceid=')[1].split('&')[0].strip()
            if 'device=' in result: data_dic['device'] = result.split('device=')[1].split('&')[0].strip()
            if 'osVer=' in result: data_dic['osVer'] = result.split('osVer=')[1].split('&')[0].strip()
            if 'os=' in result: data_dic['os'] = result.split('os=')[1].split('&')[0].strip()
            if 'app_id=' in result: data_dic['app_id'] = result.split('app_id=')[1].split('&')[0].strip()
            if 'version=' in result: data_dic['version'] = result.split('version=')[1].split('&')[0].strip()
            if 'deviceid1=' in result: data_dic['deviceid1'] = result.split('deviceid1=')[1].split('&')[0].strip()
            if 'lang=' in result: data_dic['lang'] = result.split('lang=')[1].split('&')[0].strip()
            if 'fb_id=' in result: data_dic['fb_id'] = result.split('fb_id=')[1].split('&')[0].strip()
        else:
            data_dic['api_type'] = data.split(' /')[1].split('/ HTTP/1.1')[0].strip()
            if 'sign=' in line:data_dic['sign'] = line.split('sign=')[1].split('&')[0].strip()
            if 'session_id=' in line:data_dic['session_id'] = line.split('session_id=')[1].split('&')[0].strip()
            if 'uid=' in line:data_dic['uid'] = line.split('uid=')[1].split(' "-"')[0].strip()
        if 'purchase_data=' in line:
            line = unquote(line)
            purchase_data = eval(line.split('purchase_data=')[1].split('&')[0])
            if 'orderId' in purchase_data.keys(): data_dic['orderId'] = purchase_data.get('orderId', '')
            if 'packageName' in purchase_data.keys(): data_dic['packageName'] = purchase_data.get('packageName', '')
            if 'productId' in purchase_data.keys(): data_dic['productId'] = purchase_data.get('productId', '')
            if 'purchaseTime' in purchase_data.keys(): data_dic['purchaseTime'] = purchase_data.get('purchaseTime', '')
            if 'purchaseState' in purchase_data.keys(): data_dic['purchaseState'] = purchase_data.get('purchaseState', '')
            if 'developerPayload' in purchase_data.keys(): data_dic['developerPayload'] = purchase_data.get('developerPayload', '')
            if 'purchaseToken' in purchase_data.keys(): data_dic['purchaseToken'] = purchase_data.get('purchaseToken', '')
            if 'data_signature' in line : data_dic['data_signature'] = line.split('data_signature=')[1].split(' "-"')[0]
        parse_list = [data_dic[key] for key in data_list]
        parsed_line = '\t'.join(map(str,parse_list)) + '\n'
        return parsed_line
