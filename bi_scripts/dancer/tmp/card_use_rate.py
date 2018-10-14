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
def dis_card_use_rate(date):
    sql = '''
    SELECT user_id,
           vip as vip_level,
           card_dict
    FROM mid_info_all
    WHERE ds = '{date}'
    '''.format(**{
        'date': date,
    })
    df = hql_to_df(sql)

    info_config = get_config('character_info')
    detail_config = get_config('character_detail')

    card_pos = set(range(1, 10, 1))
    def card_career_lines():
        for _, row in df.iterrows():
            for cards_id, card_info in eval(row['card_dict']).iteritems():
                card_id = cards_id.split('-')[0]
                character_id = str(detail_config.get(card_id, {}).get('character_id'))
                card_name = info_config.get(character_id, {}).get('name')
                attend_num = card_info['pos'] in card_pos
                yield [row.user_id, row.vip_level, character_id, card_name, attend_num]
    card_info_df = pd.DataFrame(card_career_lines(),columns=['user_id', 'vip_level', 'character_id', 'card_name', 'attend_num'])
    use_df = card_info_df[card_info_df['attend_num'] == 1].groupby(['character_id', 'card_name', 'vip_level']).count().reset_index().drop('user_id', axis=1)
    all_df = card_info_df.groupby(['character_id', 'card_name', 'vip_level']).agg({'user_id': lambda g: g.nunique(),
                                                                              'attend_num': lambda g: g.count(),
                                                                            }).reset_index()
    all_df['ds'] = date
    all_df = all_df.rename(columns={
        'user_id': 'have_user_num',
        'attend_num': 'total_num'
    })
    result_df = all_df.merge(use_df,on=['character_id', 'card_name', 'vip_level'],how='left')
    columns = ['ds', 'vip_level', 'character_id', 'card_name', 'have_user_num', 'total_num', 'attend_num'] # 去掉use_rate字段，在前端计算
    result_df = result_df[columns].sort_values('attend_num', ascending=False).fillna(0)

    return result_df

if __name__ == '__main__':
    settings_dev.set_env('dancer_tw')
    result = dis_card_use_rate('20161026')
    result.to_excel('/home/kaiqigu/Documents/card_use_rate.xlsx')
