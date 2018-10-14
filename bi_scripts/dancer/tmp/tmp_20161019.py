#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Hu Chunlong
Description : 卡牌使用率
Database    : dancer_ks
'''
import settings_dev
import pandas as pd
from utils import ds_add, hql_to_df, update_mysql, get_config, date_range

if __name__ == '__main__':
    settings_dev.set_env('dancer_tw')
    sql = '''
    SELECT user_id,
           vip AS vip_level,
           card_dict
    FROM parse_info
    WHERE ds ='20161015'
      AND user_id = 'tw302619864'
    '''
    df = hql_to_df(sql)

    detail_config = get_config('character_info')
    card_pos = set(range(1, 10, 1))
    def card_career_lines():
        for _, row in df.iterrows():
            for cards_id, card_info in eval(row['card_dict']).iteritems():
                card_id = cards_id.split('-')[0]
                card_name = detail_config.get(card_id, {}).get('name')
                attend_num = card_info['pos'] in card_pos
                yield [row.user_id, row.vip_level, card_id, card_name, attend_num]
    card_info_df = pd.DataFrame(card_career_lines(),columns=['user_id', 'vip_level', 'card_id', 'card_name', 'attend_num'])
    use_df = card_info_df[card_info_df['attend_num'] == 1].groupby(['card_id', 'card_name', 'vip_level']).count().reset_index().drop('user_id', axis=1)
    all_df = card_info_df.groupby(['card_id', 'card_name', 'vip_level']).agg({'user_id': lambda g: g.nunique(),
                                                                              'attend_num': lambda g: g.count(),
                                                                            }).reset_index()

