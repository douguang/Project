#!/usr/env/python
# -*- coding:utf-8 -*-
'''
Author      : Lan Xuliu
Description : 卡牌使用率
'''
import settings_dev
import pandas as pd
from utils import ds_add, hql_to_df, update_mysql, get_config, date_range

def dis_act_3day_card_using_rate(date):
    # table = 'dis_act_3day_card_using_rate'
    table = 'dis_card_use_rate'
    print table
    pre_3day = ds_add(date, -2)
    dis_card_using_sql = '''
    SELECT reverse(substr(reverse(user_id),8)) AS server,
           user_id,
           vip,
           card_dict
    FROM mid_info_all
    WHERE ds = '{date}'
      AND regexp_replace(substr(act_time,1,10),'-','') >= '{pre_3day}'
    '''.format(**{'date': date,
                  'pre_3day': pre_3day})

    print dis_card_using_sql
    dis_card_using_df = hql_to_df(dis_card_using_sql)
    character_detail_config = get_config('character_detail')
    card_shangzheng_pos = set(range(1, 10, 1))

    #print card_shangzheng_pos

    def card_career_lines():
        for _, row in dis_card_using_df.iterrows():
            for card_id, card_info in eval(row['card_dict']).iteritems():
                c_id = card_id.split('-')[0]
                character_id = character_detail_config.get(c_id, {}).get(
                    'character_ID', '-99')
                c_name = character_detail_config.get(c_id, {}).get('name')
                is_notice = character_detail_config.get(c_id, {}).get(
                    'is_notice', '-99')
                if int(is_notice) >= 2:
                    # quality = character_detail_config.get(c_id, {}).get(
                    #     'quality', -99)
                    is_shangzheng = card_info['pos'] in card_shangzheng_pos
                    yield [row.server, row.vip, row.user_id, character_id,
                           c_id, c_name, is_shangzheng]

    card_info_df = pd.DataFrame(
        card_career_lines(),
        columns=['server', 'vip', 'user_id', 'character_id', 'c_id', 'c_name',
                 'is_shangzheng'])

    grouped_result_df0 = card_info_df[card_info_df[
        'is_shangzheng'] == 1].groupby(
            ['vip', 'character_id', 'c_name']).agg({
                'user_id': lambda g: g.nunique(),
            }).reset_index().rename(columns={'user_id': 'have_user_pos'})

    grouped_result_df1 = card_info_df.groupby(
        ['vip', 'character_id', 'c_name']).agg({
            'user_id': lambda g: g.nunique(),
            'c_id': lambda g: g.count(),
        }).reset_index()

    grouped_result_all_df = grouped_result_df1.merge(
        grouped_result_df0,
        on=['vip', 'character_id', 'c_name'])
    grouped_result_all_df['ds'] = date

    columns = [
        'ds',
        # 'server',
        'vip',
        'character_id',
        'c_name',
        'c_id',
        'user_id',
        'have_user_pos',
    ]

    grouped_result_dis_df = grouped_result_all_df[columns].rename(columns={
        'c_id': 'have_num',
        'user_id': 'have_user_num',
    })
    rename_dic = {'c_name': 'card_name',
                  'have_num': 'total_num',
                  'have_user_pos': 'attend_num',
                  'have_rate': 'use_rate',
                  'vip': 'vip_level',
                  'character_id': 'card_id'}
    grouped_result_dis_df = grouped_result_dis_df.rename(columns=rename_dic)
    #print grouped_result_dis_df
    del_sql = 'delete from {0} where ds="{1}"'.format(table, date)
    update_mysql(table, grouped_result_dis_df, del_sql)


if __name__ == '__main__':
    for platform in ['metal_test']:
        settings_dev.set_env(platform)
        for date in date_range('20170206', '20170207'):
            dis_act_3day_card_using_rate(date)
