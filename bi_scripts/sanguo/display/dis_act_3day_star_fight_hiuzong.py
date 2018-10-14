#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Lan Xuliu
Desctiption : 将星信息
'''
import pandas as pd
from utils import hql_to_df, update_mysql, ds_add
from collections import Counter
import settings_dev
from pandas import DataFrame

star_name_data= {
'c_id':[506,
        505,
        504,
        503,
        502,
        501,
        406,
        405,
        404,
        403,
        402,
        401,
        306,
        305,
        304,
        303,
        302,
        301],
'name':[u'灵禄',
        u'神禄',
        u'鬼禄',
        u'人禄',
        u'地禄',
        u'天禄',
        u'灵灭',
        u'神灭',
        u'鬼灭',
        u'人灭',
        u'地灭',
        u'天灭',
        u'灵佑',
        u'神佑',
        u'鬼佑',
        u'人佑',
        u'地佑',
        u'天佑']
}
star_name_df = DataFrame(star_name_data)

def dis_act_3day_star_fight_hiuzong(date):
    pre_3day = ds_add(date, -2)
    star_dict_sql = '''
    SELECT vip,
           user_id,
           star_dict
    FROM mid_info_all
    WHERE ds = '{date}'
      AND regexp_replace(substr(act_time,1,10),'-','') >= '{pre_3day}'
    '''.format(**{'date': date,
                  'pre_3day': pre_3day})
    star_dict_df = hql_to_df(star_dict_sql)
    star_dict_df.columns = ['vip', 'user_id', 'star_dict']
    print star_dict_sql
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
                    yield [row.vip, row.user_id, c_id, is_shangzhen, lv]

    star_dict_info_df = pd.DataFrame(
        star_dict_lines(),
        columns=['vip', 'user_id', 'c_id', 'is_shangzhen', 'lv'])

    # pos_uv
    star_dict_pos_df0 = star_dict_info_df[star_dict_info_df['is_shangzhen'] >
                                          0].groupby(['vip', 'c_id']).agg({
                                              'user_id': lambda g: g.nunique(),
                                              'lv': lambda g: tuple(g),
                                          }).reset_index().rename(columns={
                                              'user_id': 'have_user_shangzhen'
                                          })
    # uv and pos_pv
    star_dict_info_df1 = star_dict_info_df.groupby(['vip', 'c_id']).agg({
        'user_id': lambda g: g.nunique(),
        'is_shangzhen': lambda g: g.sum(),
    }).reset_index().rename(columns={
        'user_id': 'have_user',
        'is_shangzhen': 'have_num_shangzhen'
    })
    # pv
    star_dict_info_df2 = star_dict_info_df.groupby(['vip', 'c_id']).agg({
        'user_id': lambda g: g.count(),
    }).reset_index().rename(columns={
        'user_id': 'have_num',
    })

    star_dict_pos_df0['lv_counter'] = star_dict_pos_df0.lv.map(
        lambda x: Counter(x))

    for lv in range(31):
        star_dict_pos_df0['star_{0}'.format(lv)] = star_dict_pos_df0[
            'lv_counter'].map(lambda s: s.get(lv, 0))

    star_dict_df = star_dict_pos_df0.merge(star_dict_info_df1,
                                           on=['vip', 'c_id'])
    star_dict_all_df = star_dict_df.merge(star_dict_info_df2,
                                          on=['vip', 'c_id'])
    star_dict_all_df = star_dict_all_df.merge(star_name_df,on = 'c_id',how='left')
    star_dict_all_df['ds'] = date
    # star_dict_all_df['server'] = star_dict_all_df['user_id'].map(lambda s: s[:-7])
    columns = [
        'ds',
        'vip',
        'c_id',
        'name',
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

    star_dict_dis_df = star_dict_all_df[columns]
    #print star_dict_dis_df

    table = 'dis_act_3day_star_fight_hiuzong'
    del_sql = 'delete from {0} where ds="{1}"'.format(table, date)
    update_mysql(table, star_dict_dis_df, del_sql)

if __name__ == '__main__':
    settings_dev.set_env('sanguo_tw')
    dis_act_3day_star_fight_hiuzong('20160701')
