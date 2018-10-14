#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
@Time    : 17-6-13 下午5:26
@Author  : Andy 
@File    : card_evo_user_id.py
@Software: PyCharm
Description :   客服/策划 某个玩家的卡牌进阶
'''

import settings_dev
import pandas as pd
from utils import ds_add, hql_to_df, update_mysql, get_config, date_range
from collections import Counter


def card_evo():
    dis_card_evo_sql = '''
    SELECT ds,
           user_id,
           vip,
           card_dict
    FROM raw_info
    WHERE ds >= '20170607'
    and ds<='20170612'
    and user_id = 'th458111899'
    '''
    print dis_card_evo_sql
    dis_card_evo_df = hql_to_df(dis_card_evo_sql)
    print dis_card_evo_df

    character_detail_config = get_config('character_detail')
    card_shangzheng_pos = set(range(1, 10, 1))

    def card_evo_lines():
        for _, row in dis_card_evo_df.iterrows():
            for card_id, card_info in eval(row['card_dict']).iteritems():
                c_id = card_id.split('-')[0]
                character_id = character_detail_config.get(c_id, {}).get(
                    'character_ID', '-99')
                c_name = character_detail_config.get(c_id, {}).get('name')
                is_notice = character_detail_config.get(c_id, {}).get(
                    'is_notice', '-99')

                if int(is_notice) >= 2:
                    quality = character_detail_config.get(c_id, {}).get(
                        'quality', -99)
                    is_shangzheng = card_info['pos'] in card_shangzheng_pos
                    evo = card_info['evo'] if is_shangzheng else -1
                    yield [row.ds, row.vip, row.user_id, character_id,
                           c_name, is_shangzheng, quality, evo]

    card_info_df = pd.DataFrame(
        card_evo_lines(),
        columns=['ds', 'vip', 'user_id', 'character_id', 'c_name',
                 'is_shangzheng', 'quality', 'evo'])
    card_info_df['quality'] = card_info_df['quality'].map(lambda s: int(s))

    rename_dic = {'c_name': '卡牌名字',
                  'is_shangzheng': '是否上阵',
                  'quality': '品质',
                  'evo': '进阶数',
                  'character_id': '卡牌ID'}
    card_info_df = card_info_df.rename(columns=rename_dic)
    card_info_df.to_excel('/home/kaiqigu/桌面/机甲无双-多语言版-玩家卡牌进阶_20170613.xlsx', index=False)

if __name__ == '__main__':
    settings_dev.set_env('sanguo_tl')
    card_evo()
    print "end"
