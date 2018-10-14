#!/usr/bin/env python  
# encoding: utf-8  

""" 
@author: Andy 
@site:  
@software: PyCharm 
@file: user_card_info2.py 
@time: 17/9/27 下午6:24 
"""
import settings_dev
import pandas as pd
from utils import ds_add, hql_to_df, update_mysql, get_config, date_range
from collections import Counter


def card_evo(date):

    dis_card_evo_sql = '''
    SELECT ds,
           user_id,
           vip,
           card_dict
    FROM mid_info_all
    WHERE ds = '{date}'
    and act_time >= '2017-07-01 00:00:00'
    '''.format(**{'date': date,})
    print dis_card_evo_sql
    dis_card_evo_df = hql_to_df(dis_card_evo_sql)

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
                    yield [row.ds,row.vip, row.user_id, character_id,
                           c_name, is_shangzheng, quality, evo]

    card_info_df = pd.DataFrame(
        card_evo_lines(),
        columns=['ds', 'vip', 'user_id', 'character_id', 'c_name',
                 'is_shangzheng', 'quality', 'evo'])
    card_info_df['quality'] = card_info_df['quality'].map(lambda s: int(s))
    res_df = card_info_df[card_info_df['character_id'].isin(['7027','7028'])]
    return res_df

if __name__ == '__main__':
    settings_dev.set_env('sanguo_ks')
    res = card_evo(20170926)
    pd.DataFrame(res).to_excel(r'/Users/kaiqigu/Documents/Sanguo/机甲无双-金山-拥有大小乔玩家的UID_20170927-ks.xlsx', index=False)
    print 'end'