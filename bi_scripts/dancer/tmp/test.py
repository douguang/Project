#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Dong Junshuang
Description :
'''
from utils import hqls_to_dfs, update_mysql, get_config
import settings_dev
import pandas as pd


file_in = '/Users/kaiqigu/Downloads/info_20160912'

f_in = open(file_in,'r')
print f_in

for l_raw in f_in:
    l = l_raw.split(',')
    try:
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
        parsed_line = '\t'.join(map(str, [user_id, account, name, reg_time, act_time, level, vip, free_coin, charge_coin, gold, energe, cmdr_energy, honor, combat, guide, max_stage, item_dict, card_dict, equip_dict, combiner_dict, once_reward, card_assistant, combiner_in_use, card_assis_active, chips, chip_pos, equip_pos, device_mark])) + '\n'
        print type(guide)
        print parsed_line
    except:
        print 'error !!!'

f_in.close()
