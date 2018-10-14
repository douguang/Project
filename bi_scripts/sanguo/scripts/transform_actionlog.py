#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Hu Wenchao
Description : 解析行为日志成impala可以分析的格式
'''
import json
# import sys

# for line in sys.stdin:
#     values = line.split('\t')
#     print '\t'.join([values[0], values[1]])

# {"body": {"a_rst":  [{"diff": "10", "after": "8692", "obj": "Dmp:FreeMoney", "before": "8682"}, {"diff": "10000", "after": "2192855", "obj": "food", "before": "2182855"}, {"diff": "10", "after": "8692", "obj": "coin", "before": "8682"}, {"diff": "2", "after": "78", "obj": "Item@13", "before": "76"}], "a_tar": {"city_id": "243"}, "a_typ": "country_war_1.get_city_preview", "a_usr": "m120012186@m12"}, "combat": 328464, "food": 17435, "app_id": "", "app_ver": "", "coin_free": 254, "coin": 254, "device_id": "0", "account": "vivo_996eab0de3e46d5a", "level": 33, "coin_charge": 0, "log_t": "1462354217", "vip_level": 0}

with open('/home/data/sanguo_ks/action_log/action_log_20160504') as f_in, open('/home/data/sanguo_ks/action_log/action_log_20160504.tsv', 'w') as f_out:
    for line in f_in:
        try:
            l = json.loads(line)
            a_usr = l['body']['a_usr']
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
            a_rst = str(l['body']['a_rst'])
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
            f_out.write('\t'.join(map(str, [a_usr, account, coin, coin_free, coin_charge, log_t, vip_level, level, combat, food, a_typ, a_tar, FreeMoney_before, FreeMoney_after, FreeMoney_diff, Money_before, Money_after, Money_diff])))
            f_out.write('\n')
        except:
            print line
            raise

