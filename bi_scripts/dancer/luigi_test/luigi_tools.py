#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Hu Chunlong
Description :
'''
import json
from urllib import unquote


def parse_actionlog(line):
    '''将json格式的动作日志转化为 tab分割 格式
    >>> {"body": {"a_rst": [{"diff": "-3600", "after": "199832", "obj": "Dmp:Gold", "before": "203432"}, {"diff": "27", "after": "43429", "obj": "combat", "before": "43402"}, {"after": {"c_id": "6840", "level": "16"}, "obj": "Equip@68-160611185844-W3f0g4", "before": {"c_id": "6840", "level": "15"}}, {"after": {"c_id": "1430", "level": "16"}, "obj": "Equip@14-160611191055-gxY8gy", "before": {"c_id": "1430", "level": "15"}}, {"after": {"c_id": "1530", "level": "16"}, "obj": "Equip@15-160611193413-IdaEfj", "before": {"c_id": "1530", "level": "15"}}, {"after": {"c_id": "1330", "level": "16"}, "obj": "Equip@13-160611190049-UjtUFZ", "before": {"c_id": "1330", "level": "15"}}], "a_tar": {"chip_put_off": "", "formation_3": "", "formation_1": "", "formation_4": "", "auto_level_up": "2", "formation_2": "", "assistant": "-1_-1_-1_-1_-1_-1_-1_-1_-1", "chip_put_on": "", "equip_put_on": "", "mobage_id": "huawei_80086000132740658", "align": "32-160611190617-xGTDSu_7-160611184706-ba5s5G_13-160611190617-DOufET_37-160611234350-MxhZkz_26-160611190617-3Z6vyN_42-160611192238-2AyC8k_-1_-1_-1", "equip_put_off": ""}, "a_typ": "cards.cards_data_mix", "a_usr": "ad1@ad17541818"}, "log_t": "1465660800", "app_id": "13001229", "app_ver": "1.1", "device_id": "16", "param5": "huawei", "mobage_uid": "huawei_80086000132740658", "param4": "497", "param3": "0", "param2": "3", "param1": "16"}
    '''
    l = json.loads(line)
    a_usr = l['body']['a_usr']  # get
    user_id_server = a_usr.split('@')  # get
    user_id, server = max(user_id_server), min(
        user_id_server)  # 兼容后端写反的情况 # get
    account = l.get('mobage_uid', '')  # tf没有该条，有mobage_id\ # get
    log_t = l.get('log_t', '')  # get
    level = l.get('param1', '')
    vip = l.get('param2', '')
    coin_charge = l.get('param3', '')
    coin_free = l.get('param4', '')
    platform = l.get('param5', '')
    a_typ = l['body']['a_typ']  # get
    a_tar = l['body']['a_tar']  # get
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
    parsed_line = '\t'.join(map(str, [
        user_id, server, account, log_t, vip, level, coin_charge, coin_free,
        platform, a_typ, a_tar, FreeMoney_before, FreeMoney_after,
        FreeMoney_diff, Money_before, Money_after, Money_diff, return_code
    ])) + '\n'
    return parsed_line


def parse_actionlog_all(line):
    l = json.loads(line)
    a_usr = l['body']['a_usr']  # get
    user_id_server = a_usr.split('@')  # get
    user_id, server = max(user_id_server), min(
        user_id_server)  # 兼容后端写反的情况 # get
    account = l.get('mobage_uid', '')  # tf没有该条，有mobage_id\ # get
    log_t = l.get('log_t', '')  # get
    level = l.get('param1', '')
    vip = l.get('param2', '')
    coin_charge = l.get('param3', '')
    coin_free = l.get('param4', '')
    platform = l.get('param5', '')
    a_typ = l['body']['a_typ']
    a_tar = l['body']['a_tar']
    a_rst = []
    for all_obj in l['body'].get('a_rst', []):
        a_rst.append(all_obj)
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
    parsed_line = '\t'.join(map(str, [
        user_id, server, account, log_t, vip, level, coin_charge, coin_free,
        platform, a_typ, a_tar, a_rst, FreeMoney_before, FreeMoney_after,
        FreeMoney_diff, Money_before, Money_after, Money_diff, return_code
    ])) + '\n'
    return parsed_line


def parse_info(line):
    '''将json格式的info转化为 tab分割 格式
    >>>
    '''
    try:
        l = json.loads(line)
        user_id = l[0]
        account = l[1]
        name = l[2]
        reg_time = l[3]
        act_time = l[4]
        level = l[5]
        vip = l[6]
        free_coin = l[7]
        charge_coin = l[8]
        gold = l[9]
        energe = l[10]
        cmdr_energy = l[11]
        honor = l[12]
        combat = l[13]
        guide = l[14]
        max_stage = l[15]
        item_dict = l[16]
        card_dict = l[17]
        equip_dict = l[18]
        combiner_dict = l[19]
        once_reward = l[20]
        card_assistant = l[21]
        combiner_in_use = l[22]
        card_assis_active = l[23]
        chips = l[24]
        chip_pos = l[25]
        equip_pos = l[26]
        device_mark = l[27]
        emblems = l[28]
        achievements = l[29]
        regist_ip = l[30]
        contact_skill = l[31]
        stone_dict = l[32]
        books_dict = l[33]
        reward_dict = l[34]
        acupoint_dict = l[35]
        appid = l[36]
        regist_time = l[37]
        pet_dict = l[38]


        parsed_line = '\t'.join(map(
            str,
            [user_id, account, name, reg_time, act_time, level, vip, free_coin,
             charge_coin, gold, energe, cmdr_energy, honor, combat, guide,
             max_stage, item_dict, card_dict, equip_dict, combiner_dict,
             once_reward, card_assistant, combiner_in_use, card_assis_active,
             chips, chip_pos, equip_pos, device_mark, emblems, achievements,
             regist_ip, contact_skill, stone_dict, books_dict, reward_dict,
             acupoint_dict, appid, regist_time, pet_dict])) + '\n'
    except Exception, e:
        print e
        print l
    return parsed_line


# 武娘多语言info解析
def mul_parse_info(line):
    '''将json格式的info转化为 tab分割 格式
    >>>
    '''
    try:
        l = json.loads(line)
        user_id = l[0]
        account = l[1]
        name = l[2]
        reg_time = l[3]
        act_time = l[4]
        level = l[5]
        vip = l[6]
        free_coin = l[7]
        charge_coin = l[8]
        gold = l[9]
        energe = l[10]
        cmdr_energy = l[11]
        honor = l[12]
        combat = l[13]
        guide = l[14]
        max_stage = l[15]
        item_dict = l[16]
        card_dict = l[17]
        equip_dict = l[18]
        combiner_dict = l[19]
        once_reward = l[20]
        card_assistant = l[21]
        combiner_in_use = l[22]
        card_assis_active = l[23]
        chips = l[24]
        chip_pos = l[25]
        equip_pos = l[26]
        device_mark = l[27]
        emblems = l[28]
        achievements = l[29]
        regist_ip = l[30]
        contact_skill = l[31]
        stone_dict = l[32]
        books_dict = l[33]
        reward_dict = l[34]
        appid = l[35]
        regist_time = l[36]
        language_sort = l[37]
        register_lan_sort = l[38]
        parsed_line = '\t'.join(map(
            str,
            [user_id, account, name, reg_time, act_time, level, vip, free_coin,
             charge_coin, gold, energe, cmdr_energy, honor, combat, guide,
             max_stage, item_dict, card_dict, equip_dict, combiner_dict,
             once_reward, card_assistant, combiner_in_use, card_assis_active,
             chips, chip_pos, equip_pos, device_mark, emblems, achievements,
             regist_ip, contact_skill, stone_dict, books_dict, reward_dict,
             appid, regist_time, language_sort, register_lan_sort])) + '\n'
    except Exception, e:
        print e
        print l
    return parsed_line

# 国服info解析
def pub_parse_info(line):
    '''将json格式的info转化为 tab分割 格式
    >>>
    '''
    try:
        l = json.loads(line)
        user_id = l[0]
        account = l[1]
        name = l[2]
        reg_time = l[3]
        act_time = l[4]
        level = l[5]
        vip = l[6]
        free_coin = l[7]
        charge_coin = l[8]
        gold = l[9]
        energe = l[10]
        cmdr_energy = l[11]
        honor = l[12]
        combat = l[13]
        guide = l[14]
        max_stage = l[15]
        item_dict = l[16]
        card_dict = l[17]
        equip_dict = l[18]
        combiner_dict = l[19]
        once_reward = l[20]
        card_assistant = l[21]
        combiner_in_use = l[22]
        card_assis_active = l[23]
        chips = l[24]
        chip_pos = l[25]
        equip_pos = l[26]
        device_mark = l[27]
        emblems = l[28]
        achievements = l[29]
        regist_ip = l[30]
        contact_skill = l[31]
        stone_dict = l[32]
        books_dict = l[33]
        reward_dict = l[34]
        acupoint_dict = l[35]
        appid = l[36]
        regist_time = l[37]
        secre_dict = l[38]
        pet_dict = l[39]
        runes = l[40]
        ride_dic = l[41]
        horcrux = l[42]



        parsed_line = '\t'.join(map(
            str,
            [user_id, account, name, reg_time, act_time, level, vip, free_coin,
             charge_coin, gold, energe, cmdr_energy, honor, combat, guide,
             max_stage, item_dict, card_dict, equip_dict, combiner_dict,
             once_reward, card_assistant, combiner_in_use, card_assis_active,
             chips, chip_pos, equip_pos, device_mark, emblems, achievements,
             regist_ip, contact_skill, stone_dict, books_dict, reward_dict,
             acupoint_dict, appid, regist_time, secre_dict, pet_dict, runes, ride_dic, horcrux])) + '\n'
    except Exception, e:
        print e
        print l
    return parsed_line


def bt_parse_info(line):
    '''将json格式的info转化为 tab分割 格式
    >>>
    '''
    try:
        l = json.loads(line)
        user_id = l[0]
        account = l[1]
        name = l[2]
        reg_time = l[3]
        act_time = l[4]
        level = l[5]
        vip = l[6]
        free_coin = l[7]
        charge_coin = l[8]
        gold = l[9]
        energe = l[10]
        cmdr_energy = l[11]
        honor = l[12]
        combat = l[13]
        guide = l[14]
        max_stage = l[15]
        item_dict = l[16]
        card_dict = l[17]
        equip_dict = l[18]
        combiner_dict = l[19]
        once_reward = l[20]
        card_assistant = l[21]
        combiner_in_use = l[22]
        card_assis_active = l[23]
        chips = l[24]
        chip_pos = l[25]
        equip_pos = l[26]
        device_mark = l[27]
        emblems = l[28]
        achievements = l[29]
        regist_ip = l[30]
        contact_skill = l[31]
        stone_dict = l[32]
        books_dict = l[33]
        reward_dict = l[34]
        acupoint_dict = l[35]
        appid = l[36]
        regist_time = l[37]
        secre_dict = l[38]
        pet_dict = l[39]
        parsed_line = '\t'.join(map(
            str,
            [user_id, account, name, reg_time, act_time, level, vip, free_coin,
             charge_coin, gold, energe, cmdr_energy, honor, combat, guide,
             max_stage, item_dict, card_dict, equip_dict, combiner_dict,
             once_reward, card_assistant, combiner_in_use, card_assis_active,
             chips, chip_pos, equip_pos, device_mark, emblems, achievements,
             regist_ip, contact_skill, stone_dict, books_dict, reward_dict,
             acupoint_dict, appid, regist_time, secre_dict, pet_dict])) + '\n'
    except Exception, e:
        print e
        print l
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
        config_name = line.split('config_name=')[1].split('&')[0].strip()
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
    parsed_line = '\t'.join(map(
        str, [ip, time, gmt, get_post, api_type, method, user_token, account,
              guide_team, guide_id, config_name, appid, luaver, resver, ts, pt,
              appver, device, pt_chl, os, osver, device_mark, device_mem, sid,
              retassid, retry, use_port, apply_time, apply_ip, net_delay_2,
              net_delay_1])) + '\n'
    return parsed_line


def parse_voided_data(line):
    '''
    将json格式的动作日志转化为 tab分割 格式 Andy
    '''
    l = json.loads(line)
    data_dict = {'kind': '',
                 'voidedTimeMillis': '',
                 'purchaseToken': '',
                 'purchaseTimeMillis': '', }
    data_list = ['purchaseTimeMillis', 'purchaseToken', 'voidedTimeMillis',
                 'kind']
    if 'kind' in l.keys(): data_dict['kind'] = l.get('kind', '').strip()
    if 'voidedTimeMillis' in l.keys():
        data_dict['voidedTimeMillis'] = l.get('voidedTimeMillis', '').strip()
    if 'purchaseToken' in l.keys():
        data_dict['purchaseToken'] = l.get('purchaseToken', '').strip()
    if 'purchaseTimeMillis' in l.keys():
        data_dict['purchaseTimeMillis'] = l.get('purchaseTimeMillis',
                                                '').strip()
    parse_list = [data_dict[key] for key in data_list]
    parsed_line = '\t'.join(map(str, parse_list)) + '\n'
    # print parsed_line
    return parsed_line


def parse_sdk_nginx(line):
    '''解析SDK的Nginx日志，提取部分字段，并转化为 tab分割 格式
        HTTP/1.1" 200 63
    >>>
    '''
    data_dic = {'ip': '',
                'time': '',
                'get_post': '',
                'api_type': '',
                'deviceid': '',
                'device': '',
                'osVer': '',
                'os': '',
                'app_id': '',
                'version': '',
                'deviceid1': '',
                'lang': '',
                'fb_id': '',
                'sign': '',
                'session_id': '',
                'uid': '',
                'orderId': '',
                'packageName': '',
                'productId': '',
                'purchaseTime': '',
                'purchaseState': '',
                'developerPayload': '',
                'purchaseToken': '',
                'data_signature': '', }
    data_list = ['ip',
                 'time',
                 'get_post',
                 'api_type',
                 'deviceid',
                 'device',
                 'osVer',
                 'os',
                 'app_id',
                 'version',
                 'deviceid1',
                 'lang',
                 'fb_id',
                 'sign',
                 'session_id',
                 'uid',
                 'orderId',
                 'packageName',
                 'productId',
                 'purchaseTime',
                 'purchaseState',
                 'developerPayload',
                 'purchaseToken',
                 'data_signature', ]
    data = line.split('"')[1].split('"')[0].strip()
    if data != '' and data.find('http://') == -1 and data.find(
            '\\x') == -1 and data.find('CONNECT') == -1:
        data_dic['ip'] = line.split(' -')[0].strip()
        data_dic['time'] = line.split('[')[1].split(' +')[0].strip()
        data_dic['get_post'] = line.split('"')[1].split(' /')[0].strip()
        if data.find('/?') != -1:
            data_dic['api_type'] = data.split(' /')[1].split('/?')[0].strip()
            result = data.split('/?')[1].split('HTTP/1.1')[0].strip()
            if 'deviceid=' in result:
                data_dic['deviceid'] = result.split('deviceid=')[1].split('&')[
                    0].strip()
            if 'device=' in result:
                data_dic['device'] = result.split('device=')[1].split('&')[
                    0].strip()
            if 'osVer=' in result:
                data_dic['osVer'] = result.split('osVer=')[1].split('&')[
                    0].strip()
            if 'os=' in result:
                data_dic['os'] = result.split('os=')[1].split('&')[0].strip()
            if 'app_id=' in result:
                data_dic['app_id'] = result.split('app_id=')[1].split('&')[
                    0].strip()
            if 'version=' in result:
                data_dic['version'] = result.split('version=')[1].split('&')[
                    0].strip()
            if 'deviceid1=' in result:
                data_dic['deviceid1'] = result.split('deviceid1=')[1].split(
                    '&')[0].strip()
            if 'lang=' in result:
                data_dic['lang'] = result.split('lang=')[1].split('&')[
                    0].strip()
            if 'fb_id=' in result:
                data_dic['fb_id'] = result.split('fb_id=')[1].split('&')[
                    0].strip()
        else:
            data_dic['api_type'] = data.split(' /')[1].split('/ HTTP/1.1')[
                0].strip()
            if 'sign=' in line:
                data_dic['sign'] = line.split('sign=')[1].split('&')[0].strip()
            if 'session_id=' in line:
                data_dic['session_id'] = line.split('session_id=')[1].split(
                    '&')[0].strip()
            if 'uid=' in line:
                data_dic['uid'] = line.split('uid=')[1].split(' "-"')[0].strip(
                )
        if 'purchase_data=' in line:
            line = unquote(line)
            purchase_data = eval(line.split('purchase_data=')[1].split('&')[0])
            if 'orderId' in purchase_data.keys():
                data_dic['orderId'] = purchase_data.get('orderId', '')
            if 'packageName' in purchase_data.keys():
                data_dic['packageName'] = purchase_data.get('packageName', '')
            if 'productId' in purchase_data.keys():
                data_dic['productId'] = purchase_data.get('productId', '')
            if 'purchaseTime' in purchase_data.keys():
                data_dic['purchaseTime'] = purchase_data.get('purchaseTime',
                                                             '')
            if 'purchaseState' in purchase_data.keys():
                data_dic['purchaseState'] = purchase_data.get('purchaseState',
                                                              '')
            if 'developerPayload' in purchase_data.keys():
                data_dic['developerPayload'] = purchase_data.get(
                    'developerPayload', '')
            if 'purchaseToken' in purchase_data.keys():
                data_dic['purchaseToken'] = purchase_data.get('purchaseToken',
                                                              '')
            if 'data_signature' in line:
                data_dic['data_signature'] = line.split('data_signature=')[
                    1].split(' "-"')[0]
        parse_list = [data_dic[key] for key in data_list]
        parsed_line = '\t'.join(map(str, parse_list)) + '\n'
        return parsed_line


if __name__ == "__main__":
    import doctest
    doctest.testmod()
