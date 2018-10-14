#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Lan Xuliu
Desctiption : 单将星分服信息
'''
import pandas as pd
from utils import hql_to_df, update_mysql, ds_add
from collections import Counter
import settings_dev


def dis_act_3day_star_fight(date):
    table = 'dis_act_3day_star_fight'
    print table
    pre_3day = ds_add(date, -2)
    star_dict_sql = '''
SELECT reverse(substr(reverse(user_id),8)) AS server,
       user_id,
       vip,
       star_dict
FROM mid_info_all
WHERE ds = '{date}'
  AND regexp_replace(substr(act_time,1,10),'-','') >= '{pre_3day}'
    '''.format(**{'date': date,
                  'pre_3day': pre_3day})
    print star_dict_sql
    star_dict_df = hql_to_df(star_dict_sql)

    dau_in_3day_df = star_dict_df.groupby(['server', 'vip']).count(
    ).reset_index()[
        ['vip', 'server', 'user_id']].rename(
            columns={'user_id': 'dau_in_3day'})

    def star_dict_lines():
        for _, row in star_dict_df.iterrows():
            for unique_id, v_info in eval(row.star_dict).iteritems():
                quality = v_info.get('quality', 0)
                c_id = v_info.get('c_id', -99)
                pos = v_info.get('pos', -99)
                if int(pos) > 0:
                    is_shangzhen = 1
                else:
                    is_shangzhen = 0
                if int(quality) >= 5:
                    lv = v_info.get('lv', 0)
                    yield [row.server, row.vip, row.user_id, c_id,
                           is_shangzhen, lv]

    star_dict_info_df = pd.DataFrame(
        star_dict_lines(),
        columns=['server', 'vip', 'user_id', 'c_id', 'is_shangzhen', 'lv'])

    # pos_uv
    star_dict_pos_df0 = star_dict_info_df[star_dict_info_df[
        'is_shangzhen'] > 0].groupby(['server', 'vip', 'c_id']).agg({
            'user_id': lambda g: g.nunique(),
            'lv': lambda g: tuple(g),
        }).reset_index().rename(columns={
            'user_id': 'have_user_shangzhen'
        })

    # uv and pos_pv
    star_dict_info_df1 = star_dict_info_df.groupby(
        ['server', 'vip', 'c_id']).agg({
            'user_id': lambda g: g.nunique(),
            'is_shangzhen': lambda g: g.sum(),
        }).reset_index().rename(columns={
            'user_id': 'have_user',
            'is_shangzhen': 'have_num_shangzhen'
        })
    # pv
    star_dict_info_df2 = star_dict_info_df.groupby(
        ['server', 'vip', 'c_id']).agg({
            'user_id': lambda g: g.count(),
        }).reset_index().rename(columns={
            'user_id': 'have_num',
        })

    star_dict_pos_df0['lv_counter'] = star_dict_pos_df0.lv.map(
        lambda x: Counter(x))

    for lv in range(31):
        star_dict_pos_df0['star_{0}'.format(lv)] = star_dict_pos_df0[
            'lv_counter'].map(lambda s: s.get(lv, 0))

    star_dict_df0 = star_dict_pos_df0.merge(star_dict_info_df1,
                                            on=['server', 'vip', 'c_id'])
    star_dict_df = star_dict_df0.merge(star_dict_info_df2,
                                       on=['server', 'vip', 'c_id'])
    star_dict_all_df = star_dict_df.merge(dau_in_3day_df, on=['server', 'vip'])
    star_dict_all_df['ds'] = date

    columns = [
        'ds',
        'server',
        'vip',
        'dau_in_3day',
        'c_id',
        'have_user',
        'have_num',
        'have_user_shangzhen',
        'have_num_shangzhen',
        'star_0',
        'star_1',
        'star_2',
        'star_3',
        'star_4',
        'star_5',
        'star_6',
        'star_7',
        'star_8',
        'star_9',
        'star_10',
        'star_11',
        'star_12',
        'star_13',
        'star_14',
        'star_15',
        'star_16',
        'star_17',
        'star_18',
        'star_19',
        'star_10',
        'star_20',
        'star_21',
        'star_22',
        'star_23',
        'star_24',
        'star_25',
        'star_26',
        'star_27',
        'star_28',
        'star_29',
        'star_30',
    ]

    star_dict_dis_df = star_dict_all_df[columns].rename(
        columns={'dau_in_3day': 'dau'})
    #print star_dict_dis_df

    del_sql = 'delete from {0} where ds="{1}"'.format(table, date)
    update_mysql(table, star_dict_dis_df, del_sql)


if __name__ == '__main__':
    settings_dev.set_env('sanguo_ks')
    dis_act_3day_star_fight('20160524')
