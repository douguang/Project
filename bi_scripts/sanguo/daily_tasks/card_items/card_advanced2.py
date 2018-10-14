#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
@Time    : 17-6-13 下午12:26
@Author  : Andy 
@File    : card_advanced.py
@Software: PyCharm
Description :   卡牌进阶  查询谋各玩家的卡牌进阶情况
'''
import settings_dev
import pandas as pd
from utils import ds_add, hql_to_df, update_mysql, get_config, date_range
from collections import Counter


def card_evo(user_id,card_list,date,end_date):

    pre_3day = ds_add(date, -2)
    # print pre_3day
    dis_card_evo_sql = '''
    SELECT ds,
           user_id,
           vip,
           card_dict
    FROM raw_info
    WHERE ds >= '{date}'
      AND ds<='{end_date}'
      and user_id = '{user_id}'
    '''.format(**{'date': date,'end_date': end_date,'user_id':user_id})
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
                    yield [row.server, row.vip, row.user_id, character_id,
                           c_name, is_shangzheng, quality, evo]

    card_info_df = pd.DataFrame(
        card_evo_lines(),
        columns=['ds', 'vip', 'user_id', 'character_id', 'c_name',
                 'is_shangzheng', 'quality', 'evo'])
    card_info_df['quality'] = card_info_df['quality'].map(lambda s: int(s))
    grouped_result_df0 = card_info_df.groupby(
        ['server', 'vip', 'character_id', 'c_name']).agg({
            'user_id': lambda g: g.nunique(),
            'is_shangzheng': lambda g: g.sum(),
            'evo': lambda g: tuple(g),
        }).reset_index()

    grouped_result_df1 = card_info_df[card_info_df['quality'] == 5].groupby(
        ['server', 'vip', 'character_id', 'c_name']).agg({
            'evo': lambda g: tuple(g),
        }).reset_index()

    grouped_result_df2 = card_info_df[card_info_df['quality'] == 6].groupby(
        ['server', 'vip', 'character_id', 'c_name']).agg({
            'evo': lambda g: tuple(g),
        }).reset_index()

    grouped_result_df1['evo_counter'] = grouped_result_df1.evo.map(
        lambda x: Counter(x))

    grouped_result_df2['evo_counter'] = grouped_result_df2.evo.map(
        lambda x: Counter(x))

    for evo in range(4):
        grouped_result_df1['orange_{0}'.format(evo)] = grouped_result_df1[
            'evo_counter'].map(lambda x: x.get(evo, 0))

    for evo in range(41):
        grouped_result_df2['red_{0}'.format(evo)] = grouped_result_df2[
            'evo_counter'].map(lambda x: x.get(evo, 0))

    grouped_result_df = grouped_result_df1.merge(
        grouped_result_df2,
        on=['vip', 'character_id', 'c_name'],how='outer')
    grouped_result_all_df = grouped_result_df0.merge(
        grouped_result_df,
        on=['vip', 'character_id', 'c_name'],how='outer')
    grouped_result_all_df['ds'] = date
    grouped_result_all_df = pd.DataFrame(grouped_result_all_df).fillna(0)
    grouped_result_all_df = grouped_result_all_df.replace(value=0,to_replace='None')
    coloumns = [
        'ds',
        'vip',
        'character_id',
        'c_name',
        'user_id',
        'is_shangzheng',
        'orange_0',
        'orange_1',
        'orange_2',
        'orange_3',
        'red_0',
        'red_1',
        'red_2',
        'red_3',
        'red_4',
        'red_5',
        'red_6',
        'red_7',
        'red_8',
        'red_9',
        'red_10',
        'red_11',
        'red_12',
        'red_13',
        'red_14',
        'red_15',
        'red_16',
        'red_17',
        'red_18',
        'red_19',
        'red_20',
        'red_21',
        'red_22',
        'red_23',
        'red_24',
        'red_25',
        'red_26',
        'red_27',
        'red_28',
        'red_29',
        'red_30',
        'red_31',
        'red_32',
        'red_33',
        'red_34',
        'red_35',
        'red_36',
        'red_37',
        'red_38',
        'red_39',
        'red_40',
    ]

    dis_card_evo_df = grouped_result_all_df[coloumns].rename(columns={
        'user_id': 'have_user_num',
        'is_shangzheng': 'shangzheng_num'
    })
    print dis_card_evo_df
    rename_dic = {'c_name': 'card_name',
                  'shangzheng_num': 'attend_num',
                  'vip': 'vip_level',
                  'character_id': 'card_id'}
    dis_card_evo_df = dis_card_evo_df.rename(columns=rename_dic)
    dis_card_evo_df.to_excel(r'/home/kaiqigu/桌面/机甲无双-多语言-玩家卡牌进阶_20170613.xlsx', index=False)

if __name__ == '__main__':
    settings_dev.set_env('sanguo_tl')
    for date in date_range('20170107', '20170122'):
        card_evo(date)

