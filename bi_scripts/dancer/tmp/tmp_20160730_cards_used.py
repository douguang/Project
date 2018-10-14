#!/usr/env/python
# -*- coding:utf-8 -*-
'''
Author      : Lan Xuliu
Description : 卡牌使用率
'''
import settings_dev
import pandas as pd
from utils import ds_add, hql_to_df, update_mysql, get_config

def dis_act_3day_card_using_rate():
    dis_card_using_sql = '''
    SELECT user_id,
           card_dict
    FROM mid_info_all
    WHERE ds = '20160815'
      AND user_id IN
        (SELECT user_id
         FROM mid_actionlog
         WHERE ds = '20160808')
    '''
    print dis_card_using_sql
    dis_card_using_df = hql_to_df(dis_card_using_sql)
    # character_detail_config = get_config('character_detail')
    card_shangzheng_pos = set(range(1, 10, 1))
    print card_shangzheng_pos
    def card_career_lines():
        for _, row in dis_card_using_df.iterrows():
            for card_id, card_info in eval(row['card_dict']).iteritems():
                c_id = card_id.split('-')[0]
                is_shangzheng = card_info['pos'] in card_shangzheng_pos
                yield [row.user_id, c_id, is_shangzheng]

    card_info_df = pd.DataFrame(card_career_lines(),columns=['user_id', 'c_id', 'is_shangzheng'])
    print card_info_df
    use_df = card_info_df[card_info_df['is_shangzheng'] == 1].groupby('c_id').count().reset_index().drop('user_id', axis=1)
    print use_df
    all_df = card_info_df.groupby('c_id').agg({'user_id': lambda g: g.nunique(),
                                               'is_shangzheng': lambda g: g.count(),
                                             }).reset_index()
    print all_df
    use_df = use_df.rename(columns={'is_shangzheng': 'used'})
    result_df = all_df.merge(use_df,on='c_id',how='left')
    result_df.to_excel('/Users/kaiqigu/Documents/dancer/tmp_20160819_card_use.xlsx')
if __name__ == '__main__':
    settings_dev.set_env('dancer_tw')
    dis_act_3day_card_using_rate()
