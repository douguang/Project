#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      :
Description : 明教卡牌
Database    :
'''
import settings_dev
import pandas as pd
from utils import ds_add, hql_to_df, update_mysql, get_config, date_range

if __name__ == '__main__':
    settings_dev.set_env('dancer_tw')
    date = '20160921'
# def dis_card_use_rate(date):
    sql = '''
    SELECT user_id,
           vip as vip_level,
           card_dict
    FROM parse_info
    WHERE ds = '{date}'
    '''.format(**{
        'date': date,
    })
    df = hql_to_df(sql)

    # detail_config = get_config('character_info')
    # card_pos = set(range(1, 10, 1))
    def card_career_lines():
        for _, row in df.iterrows():
            for cards_id, card_info in eval(row['card_dict']).iteritems():
                card_id = cards_id.split('-')[0]
                num = len(card_id)
                card = card_id[:1]
                # card_name = detail_config.get(card_id, {}).get('name')
                # attend_num = card_info['pos'] in card_pos
                yield [row.user_id, row.vip_level, card_id, num, card]
    card_info_df = pd.DataFrame(card_career_lines(),columns=['user_id', 'vip_level', 'card_id','num', 'card'])

    # card_info_df['card'] = card_info_df['card'].map(lambda s: int(s))
    data = card_info_df[card_info_df.num == 4]
    data = data[data.card == '5']
    print data
    # card_info_df
    # dt = card_info_df.loc[card_info_df.card_id == 5500]
    # dt1 = card_info_df.loc[card_info_df.card_id == 5]

    # print dt
    # print dt1

    # use_df = card_info_df[card_info_df['attend_num'] == 1].groupby(['card_id', 'card_name', 'vip_level']).count().reset_index().drop('user_id', axis=1)
    # all_df = card_info_df.groupby(['card_id', 'card_name', 'vip_level']).agg({'user_id': lambda g: g.nunique(),
    #                                                                           'attend_num': lambda g: g.count(),
    #                                                                         }).reset_index()
    # all_df['ds'] = date
    # all_df = all_df.rename(columns={
    #     'user_id': 'have_user_num',
    #     'attend_num': 'total_num'
    # })
    # result_df = all_df.merge(use_df,on=['card_id', 'card_name', 'vip_level'],how='left')
    # columns = ['ds', 'vip_level', 'card_id', 'card_name', 'have_user_num', 'total_num', 'attend_num'] # 去掉use_rate字段，在前端计算
    # result_df = result_df[columns].sort_values('attend_num', ascending=False).fillna(0)
    # #更新MySQL
    # table = 'dis_card_use_rate'
    # print date,table
    # del_sql = 'delete from {0} where ds="{1}"'.format(table, date)
    # update_mysql(table, result_df, del_sql)
# if __name__ == '__main__':
#     settings_dev.set_env('dancer_tw')
#     for date in date_range('20160913', '20160920'):
#         dis_card_use_rate(date)
