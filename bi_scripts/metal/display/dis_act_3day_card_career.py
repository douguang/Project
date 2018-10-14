#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Lan Xuliu
Description : 卡牌转职
Name        : dis_d3_act_user_num
Original    : dis_act_3day_card_career
'''
import settings_dev
import pandas as pd
from utils import ds_add, hql_to_df, update_mysql, get_config
from collections import Counter


def dis_act_3day_card_career(date):
    table = 'dis_act_3day_card_career'
    pre_3day = ds_add(date, -2)

    dis_card_career_sql = '''
    SELECT reverse(substr(reverse(user_id),8)) AS server,
           user_id,
           vip,
           card_dict
    FROM mid_info_all
    WHERE ds = '{date}'
      AND regexp_replace(substr(act_time,1,10),'-','') >= '{pre_3day}'
    '''.format(**{'date': date,
              'pre_3day': pre_3day})

    dis_card_career_df = hql_to_df(dis_card_career_sql)

    character_detail_config = get_config('character_detail')

    card_shangzheng_pos = set(range(1, 10, 1))

    def card_career_lines():
        for _, row in dis_card_career_df.iterrows():
            for card_id, card_info in eval(row['card_dict']).iteritems():
                c_id = card_id.split('-')[0]
                character_id = character_detail_config.get(c_id, {}).get(
                    'character_ID', '-99')
                c_name = character_detail_config.get(c_id, {}).get('name')
                is_notice = character_detail_config.get(c_id, {}).get(
                    'is_notice', '-99')
                if int(is_notice) >= 2:
                    is_shangzheng = card_info['pos'] in card_shangzheng_pos
                    career_lv = card_info['career_lv'] if is_shangzheng else -1
                    yield [row.server, row.vip, row.user_id, character_id,
                           c_name, is_shangzheng, career_lv]

    card_info_df = pd.DataFrame(
        card_career_lines(),
        columns=['server', 'vip', 'user_id', 'character_id', 'c_name',
                 'is_shangzheng', 'career_lv'])

    grouped_result_df0 = card_info_df[card_info_df[
        'is_shangzheng'] == 1].groupby(
            ['server', 'vip', 'character_id', 'c_name']).agg({
                'user_id': lambda g: g.nunique(),
            }).reset_index().rename(columns={'user_id': 'have_user_pos'})

    grouped_result_df1 = card_info_df.groupby(
        ['server', 'vip', 'character_id', 'c_name']).agg({
            'user_id': lambda g: g.nunique(),
            'career_lv': lambda g: tuple(g),
        }).reset_index()

    grouped_result_df1['career_lv_counter'] = grouped_result_df1.career_lv.map(
        lambda x: Counter(x))

    for lv in range(16):
        grouped_result_df1['career_{0}'.format(lv)] = grouped_result_df1[
            'career_lv_counter'].map(lambda x: x.get(lv, 0))

    grouped_result_all_df = grouped_result_df1.merge(
        grouped_result_df0,
        on=['server', 'vip', 'character_id', 'c_name'])

    grouped_result_all_df['ds'] = date

    columns = [
        'ds',
        'server',
        'vip',
        'character_id',
        'c_name',
        'user_id',
        'have_user_pos',
        'career_0',
        'career_1',
        'career_2',
        'career_3',
        'career_4',
        'career_5',
        'career_6',
        'career_7',
        'career_8',
        'career_9',
        'career_10',
        'career_11',
        'career_12',
        'career_13',
        'career_14',
        'career_15',
    ]

    dis_card_career_df = grouped_result_all_df[columns].rename(columns={
        'user_id': 'have_user_num'
    })
    print dis_card_career_df
    del_sql = 'delete from {0} where ds="{1}"'.format(table, date)
    update_mysql(table, dis_card_career_df, del_sql)


if __name__ == '__main__':
    settings_dev.set_env('metal_test')
    dis_act_3day_card_career('20160517')
