#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Dong Junshuang
Description : 卡牌飞升
'''
import settings_dev
import pandas as pd
from utils import ds_add, hql_to_df, update_mysql, get_config, date_range

settings_dev.set_env('dancer_tw')
card_sql = '''
SELECT user_id,
       vip ,
       card_dict
FROM parse_info
WHERE ds = '20161010'
and user_id in (
'tw01961047'
)
'''
card_df = hql_to_df(card_sql)

def card_career_lines():
    for _, row in card_df.iterrows():
        for cards_id, card_info in eval(row['card_dict']).iteritems():
            # print cards_id, card_info
            card_id = cards_id.split('-')[0]
            # print card_id
            evo_num = card_info.get('evo', {})
            # print evo_num
            yield [row.user_id, card_id, evo_num]
card_info_df = pd.DataFrame(card_career_lines(),columns=['user_id', 'card_id', 'evo_num'])

card_info_df['card_id'] = card_info_df['card_id'].map(lambda s :int(s))


card_info_df.to_excel('/Users/kaiqigu/Downloads/Excel/feisheng_yongchun.xlsx')
