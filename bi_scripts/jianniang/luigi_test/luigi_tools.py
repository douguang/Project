#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Dong Junshuang
Description :
Time        : 2017.03.16
'''
import json
# from lib.utils import Parse_data


def parse_actionlog(line):
    '''将json格式的动作日志转化为 tab分割 格式
    '''
    l = json.loads(line)
    a_usr = l['body']['a_usr']
    user_id_server = a_usr.split('@')
    user_id, server = max(user_id_server), min(user_id_server)  # 兼容后端写反的情况
    account = l.get('account', '')
    platform = l.get('platform', '')  # 平台
    appid = l.get('app_id', '')  # 包名
    log_t = l.get('log_t', '')  # 时间戳
    a_typ = l['body']['a_typ']
    a_tar = l['body']['a_tar']
    a_rst = l['body'].get('a_rst', [])
    return_code = l['body'].get('return_code', '')
    uuid = a_tar.get('uuid', '')
    session_id = a_tar.get('login_id', '')
    coin_before = coin_after = coin_diff = ''
    for obj in a_rst:
        if obj['obj'] == 'coin':
            coin_before = obj['before']
            coin_after = obj['after']
            coin_diff = obj['diff']
    parsed_line = '\t'.join(map(str, [
        user_id, server, account, platform, appid, log_t, uuid, coin_before,
        coin_after, coin_diff, a_typ, a_tar, a_rst, return_code, session_id
    ])) + '\n'
    return parsed_line

# f_in = '/Users/kaiqigu/Documents/Excel/H5_data/action_log_20170316'
# f_out = '/Users/kaiqigu/Documents/Excel/H5_data/parse_action_log_20170316'
# Parse_data(f_in, f_out, parse_actionlog)


def parse_nginx(line):
    '''解析Nginx日志，提取部分字段，并转化为 tab分割 格式
    >>>
    '''
    ip = line.split(' -')[0].strip()
    time = line.split('[')[1].split(' +')[0].strip()
    gmt = line.split('+')[1].split(']')[0].strip()
    get_post = line.split('"')[1].split(' /')[0].strip()
    use_port = line.split('" ')[1].split(' ')[0].strip()
    apply_time = line.split('" ')[1].split(' ')[1].strip()
    if 'var=' in line:
        var = line.split('var=')[1].split('&')[0].strip()
    else:
        var = ''
    if 'logName=' in line:
        logName = line.split('logName=')[1].split('&')[0].strip()
    else:
        logName = ''
    if 'recordType=' in line:
        recordType = line.split('recordType=')[1].split('&')[0].strip()
    else:
        recordType = ''
    if 'recordTypey=' in line:
        recordTypey = line.split('recordTypey=')[1].split('&')[0].strip()
    else:
        recordTypey = ''
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
        deviceTime = line.split('deviceTime=')[1].split('&')[0].strip()
    else:
        deviceTime = ''
    if 'deviceNet=' in line:
        deviceNet = line.split('deviceNet=')[1].split('&')[0].strip()
    else:
        deviceNet = ''

    parsed_line = '\t'.join(map(str, [ip, time, gmt, get_post, var, logName, recordType, recordTypey, channel, deviceMac, deviceName, deviceVersion, deviceTime, deviceNet, use_port, apply_time])) + '\n'
    return parsed_line


def parse_accesslog(line):
    '''解析Nginx日志，提取部分字段，并转化为 tab分割 格式
    >>>
    '''
    ip = line.split(' -')[0].strip()
    time = line.split('[')[1].split(' +')[0].strip()
    gmt = line.split('+')[1].split(']')[0].strip()
    get_post = line.split('"')[1].split(' /')[0].strip()
    use_port = line.split('" ')[1].split(' ')[0].strip()
    apply_time = line.split('" ')[1].split(' ')[1].strip()
    if 'pt=' in line:
        pt = line.split('pt=')[1].split('&')[0].strip()
    else:
        pt = ''
    if 'rc=' in line:
        rc = line.split('rc=')[1].split('&')[0].strip()
    else:
        rc = ''
    if 'platformGameId=' in line:
        platformGameId = line.split('platformGameId=')[1].split('&')[0].strip()
    else:
        platformGameId = ''
    if 'userId=' in line:
        userId = line.split('userId=')[1].split('&')[0].strip()
    elif 'login_id=' in line:
        userId = line.split('login_id=')[1].split('&')[0].strip()
    else:
        userId = ''
    # if 'channel=' in line:
    #     channel = line.split('channel=')[1].split('&')[0].strip()
    # else:
    #     channel = ''
    # if 'deviceMac=' in line:
    #     deviceMac = line.split('deviceMac=')[1].split('&')[0].strip()
    # else:
    #     deviceMac = ''
    # if 'deviceName=' in line:
    #     deviceName = line.split('deviceName=')[1].split('&')[0].strip()
    # else:
    #     deviceName = ''
    # if 'deviceVersion=' in line:
    #     deviceVersion = line.split('deviceVersion=')[1].split('&')[0].strip()
    # else:
    #     deviceVersion = ''
    # if 'deviceTime=' in line:
    #     deviceTime = line.split('deviceTime=')[1].split('&')[0].strip()
    # else:
    #     deviceTime = ''
    # if 'deviceNet=' in line:
    #     deviceNet = line.split('deviceNet=')[1].split('&')[0].strip()
    # else:
    #     deviceNet = ''
    all_data = line
    parsed_line = '\t'.join(map(str, [ip, time, gmt, get_post, pt, rc, platformGameId, userId, use_port, apply_time, all_data])) + '\n'
    return parsed_line
