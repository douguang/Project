#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      :
Description :
'''
import json
from pandas import DataFrame
file_in = r'/Users/kaiqigu/Downloads/action_log_20160921'
file_out = r'/Users/kaiqigu/Downloads/data_20160921'

# file_in = r'/home/data/dancer_tw/action_log/action_log_20160921'
# file_out = r'/home/data/dancer_tw/action_log/data_20160921'

f_in = open(file_in,'r')
f_out = open(file_out,'w')
# print f_in
# print f_out
for l_raw in f_in:
    l = json.account(l_raw)
    a_usr = l['body']['a_usr'] # get
    user_id_server = a_usr.split('@') # get
    user_id, server = max(user_id_server), min(user_id_server)  # 兼容后端写反的情况 # get
    account = l.get('mobage_uid', '') # tf没有该条，有mobage_id\ # get
    log_t = l.get('log_t', '') # get
    level = l.get('param1', '')
    vip = l.get('param2', '')
    coin_charge = l.get('param3', '')
    coin_free = l.get('param4', '')
    platform = l.get('param5', '')
    a_typ = l['body']['a_typ'] # get
    a_tar = l['body']['a_tar'] # get
    return_code = a_tar.pop('return_code', '')
    FreeMoney_before = FreeMoney_after = FreeMoney_diff = ''
    Money_before = Money_after = Money_diff = ''
    obj_without_money = []
    after_str = ''
    for obj in l['body'].get('a_rst', []):
        after = obj.get('after', [])
        after_str = after
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
    parsed_line = '\t'.join(map(str, [user_id, server, a_typ,  after_str])) + '\n'
        # return parsed_line
    f_out.write(parsed_line)
# card_data = DataFrame({'uid':uid_list,'card_id':card_id_list})
# card_data.to_csv(file_out, sep = '\t',index=False)

f_in.close()
f_out.close()


# def parse_actionlog(line):
#     '''将json格式的动作日志转化为 tab分割 格式
#     >>> {"body": {"a_rst": [{"diff": "-3600", "after": "199832", "obj": "Dmp:Gold", "before": "203432"}, {"diff": "27", "after": "43429", "obj": "combat", "before": "43402"}, {"after": {"c_id": "6840", "level": "16"}, "obj": "Equip@68-160611185844-W3f0g4", "before": {"c_id": "6840", "level": "15"}}, {"after": {"c_id": "1430", "level": "16"}, "obj": "Equip@14-160611191055-gxY8gy", "before": {"c_id": "1430", "level": "15"}}, {"after": {"c_id": "1530", "level": "16"}, "obj": "Equip@15-160611193413-IdaEfj", "before": {"c_id": "1530", "level": "15"}}, {"after": {"c_id": "1330", "level": "16"}, "obj": "Equip@13-160611190049-UjtUFZ", "before": {"c_id": "1330", "level": "15"}}], "a_tar": {"chip_put_off": "", "formation_3": "", "formation_1": "", "formation_4": "", "auto_level_up": "2", "formation_2": "", "assistant": "-1_-1_-1_-1_-1_-1_-1_-1_-1", "chip_put_on": "", "equip_put_on": "", "mobage_id": "huawei_80086000132740658", "align": "32-160611190617-xGTDSu_7-160611184706-ba5s5G_13-160611190617-DOufET_37-160611234350-MxhZkz_26-160611190617-3Z6vyN_42-160611192238-2AyC8k_-1_-1_-1", "equip_put_off": ""}, "a_typ": "cards.cards_data_mix", "a_usr": "ad1@ad17541818"}, "log_t": "1465660800", "app_id": "13001229", "app_ver": "1.1", "device_id": "16", "param5": "huawei", "mobage_uid": "huawei_80086000132740658", "param4": "497", "param3": "0", "param2": "3", "param1": "16"}
#     '''
#     l = json.loads(line)
#     a_usr = l['body']['a_usr'] # get
#     user_id_server = a_usr.split('@') # get
#     user_id, server = max(user_id_server), min(user_id_server)  # 兼容后端写反的情况 # get
#     a_typ = l['body']['a_typ'] # get
#     a_tar = l['body']['a_tar'] # get
#     a_rst = l['body']['a_rst'] # get
#     a_rst = l['body']['a_rst']

    # parsed_line = '\t'.join(map(str, [user_id, server, account, log_t, vip, level, coin_charge, coin_free, platform, a_typ, a_tar, FreeMoney_before, FreeMoney_after, FreeMoney_diff, Money_before, Money_after, Money_diff, return_code])) + '\n'
    # return parsed_line
