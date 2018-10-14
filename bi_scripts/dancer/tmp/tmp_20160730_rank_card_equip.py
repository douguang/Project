#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Hu Wenchao
Name        : combat_card_equip
'''
import settings_dev
from utils import hql_to_df, update_mysql, ds_add, date_range
from pandas import DataFrame
import pandas as pd

def combat_card_equip():

    combat_sql = '''
    SELECT user_id,
           combat,
           card_dict,
           equip_dict,
           combiner_dict
    FROM mid_info_all
    WHERE ds = '20160815'
      AND user_id IN
        (SELECT user_id
         FROM parse_info
         WHERE ds = '20160808')
    ORDER BY combat DESC LIMIT 20
    '''
    combat_df = hql_to_df(combat_sql)
    used_pos = set(range(1, 10, 1))
    c_user_id, c_combat = [], []
    e_user_id, e_combat = [], []
    m_user_id, m_combat, c_id, c_ids, c_used, c_evo, e_id, e_ids, e_used, e_evo, m_id, m_leaguer, m_quality = [], [], [], [],[], [],[], [],[], [], [], [], []
    for _, row in combat_df.iterrows():
        for card_id, card_info in eval(row['card_dict']).iteritems():
            c_user_id.append(row.user_id)
            c_combat.append(row.combat)
            c_id.append(card_id.split('-')[0])
            c_ids.append(card_info['c_id'])
            c_used.append(card_info['pos'] in used_pos)
            c_evo.append(card_info['evo'])
        for equip_id, equip_info in eval(row['equip_dict']).iteritems():
            e_user_id.append(row.user_id)
            e_combat.append(row.combat)
            e_id.append(equip_id.split('-')[0])
            e_ids.append(equip_info['c_id'])
            e_used.append(equip_info['pos'] in used_pos)
            e_evo.append(equip_info['evo'])
        for combiner_id, combiner_info in eval(row['combiner_dict']).iteritems():
            m_user_id.append(row.user_id)
            m_combat.append(row.combat)
            m_id.append(combiner_id)
            m_leaguer.append(combiner_info['leaguer'])
            m_quality.append(combiner_info['quality'])
    combat_card_df = DataFrame({'user_id': c_user_id, 'combat': c_combat, 'c_id': c_id, 'c_ids': c_ids, 'c_used': c_used, 'c_evo': c_evo})
    print combat_card_df
    combat_equip_df = DataFrame({'user_id': e_user_id, 'combat': e_combat, 'e_id': e_id, 'e_ids': e_ids, 'e_used': e_used, 'e_evo': e_evo})
    combat_combiner_df = DataFrame({'user_id': m_user_id, 'combat': m_combat, 'm_id': m_id, 'm_leaguer': m_leaguer, 'm_quality': m_quality})
    writer = pd.ExcelWriter('/Users/kaiqigu/Documents/dancer/tmp_20160819_combat_card_equip.xlsx')
    combat_card_df.to_excel(writer,'combat_card')
    combat_equip_df.to_excel(writer,'combat_equip')
    combat_combiner_df.to_excel(writer,'combat_combiner')

def level_card_equip():
    level_sql = '''
    SELECT user_id,
           level,
           card_dict,
           equip_dict,
           combiner_dict
    FROM mid_info_all
    WHERE ds = '20160815'
      AND user_id IN
        (SELECT user_id
         FROM parse_info
         WHERE ds = '20160808')
    ORDER BY level DESC LIMIT 20
    '''
    level_df = hql_to_df(level_sql)
    used_pos = set(range(1, 10, 1))
    c_user_id, c_level = [], []
    e_user_id, e_level = [], []
    m_user_id, m_level, c_id, c_ids, c_used, c_evo, e_id, e_ids, e_used, e_evo, m_id, m_leaguer, m_quality = [], [], [], [],[], [],[], [],[], [], [], [], []
    for _, row in level_df.iterrows():
        for card_id, card_info in eval(row['card_dict']).iteritems():
            c_user_id.append(row.user_id)
            c_level.append(row.level)
            c_id.append(card_id.split('-')[0])
            c_ids.append(card_info['c_id'])
            c_used.append(card_info['pos'] in used_pos)
            c_evo.append(card_info['evo'])
        for equip_id, equip_info in eval(row['equip_dict']).iteritems():
            e_user_id.append(row.user_id)
            e_level.append(row.level)
            e_id.append(equip_id.split('-')[0])
            e_ids.append(equip_info['c_id'])
            e_used.append(equip_info['pos'] in used_pos)
            e_evo.append(equip_info['evo'])
        for combiner_id, combiner_info in eval(row['combiner_dict']).iteritems():
            m_user_id.append(row.user_id)
            m_level.append(row.level)
            m_id.append(combiner_id)
            m_leaguer.append(combiner_info['leaguer'])
            m_quality.append(combiner_info['quality'])
    level_card_df = DataFrame({'user_id': c_user_id, 'level': c_level, 'c_id': c_id, 'c_ids': c_ids, 'c_used': c_used, 'c_evo': c_evo})
    level_equip_df = DataFrame({'user_id': e_user_id, 'level': e_level, 'e_id': e_id, 'e_ids': e_ids, 'e_used': e_used, 'e_evo': e_evo})
    level_combiner_df = DataFrame({'user_id': m_user_id, 'level': m_level, 'm_id': m_id, 'm_leaguer': m_leaguer, 'm_quality': m_quality})
    print level_card_df
    writer = pd.ExcelWriter('/Users/kaiqigu/Documents/dancer/tmp_20160819_level_card_equip.xlsx')
    level_card_df.to_excel(writer,'level_card')
    level_equip_df.to_excel(writer,'level_equip')
    level_combiner_df.to_excel(writer,'level_combiner')

if __name__ == '__main__':
    settings_dev.set_env('dancer_tw')
    combat_card_equip()
    level_card_equip()
