#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Lan Xuliu
Desctiption : 战姬信息
'''

import pandas as pd
from utils import hql_to_df, update_mysql, ds_add
from collections import Counter
import settings_dev


def dis_act_3day_fight_girl(date):
    table = 'dis_act_3day_fight_girl'
    print table
    d_girl = {100: '玄武', 200: '白虎', 300: '朱雀', 400: '青龙'}
    pre_3day = ds_add(date, -2)
    fight_girl_sql = '''
SELECT reverse(substr(reverse(user_id),8)) AS server,
       user_id,
       vip,
       fight_girl
FROM mid_info_all
WHERE ds = '{date}'
  AND regexp_replace(substr(act_time,1,10),'-','') >= '{pre_3day}'
'''.format(**{'date': date,
              'pre_3day': pre_3day})
    print fight_girl_sql
    fight_girl_df = hql_to_df(fight_girl_sql)
    fight_girl_df.columns = ['server', 'user_id', 'vip', 'fight_girl']

    def fight_girl_lines():
        for _, row in fight_girl_df.iterrows():
            for girl_id, v_info in eval(row.fight_girl).iteritems():
                if int(girl_id) >= 100:
                    status = v_info.get('status', 0)
                    if int(status) == 2:
                        girl_lv = v_info.get('current_level', '-99')
                        girl_name = d_girl.get(girl_id, 'ufo')
                        yield [row.server, row.vip, row.user_id, girl_id,
                               girl_name, girl_lv]

    fight_girl_info_df = pd.DataFrame(
        fight_girl_lines(),
        columns=['server', 'vip', 'user_id', 'girl_id', 'girl_name', 'girl_lv'
                 ])

    fight_girl_result_df = fight_girl_info_df.groupby(
        ['server', 'vip', 'girl_id', 'girl_name']).agg({
            'user_id': lambda g: g.nunique(),
            'girl_lv': lambda g: tuple(g)
        }).reset_index()

    fight_girl_result_df['girl_lv_counter'] = fight_girl_result_df.girl_lv.map(
        lambda x: Counter(x))

    for lv in range(41):
        fight_girl_result_df['girl_{0}'.format(lv)] = fight_girl_result_df[
            'girl_lv_counter'].map(lambda s: s.get(lv, 0))

    fight_girl_result_df['ds'] = date

    columns = [
        'ds',
        'server',
        'vip',
        'girl_id',
        'girl_name',
        'girl_0',
        'girl_1',
        'girl_2',
        'girl_3',
        'girl_4',
        'girl_5',
        'girl_6',
        'girl_7',
        'girl_8',
        'girl_9',
        'girl_10',
        'girl_11',
        'girl_12',
        'girl_13',
        'girl_14',
        'girl_15',
        'girl_16',
        'girl_17',
        'girl_18',
        'girl_19',
        'girl_20',
        'girl_21',
        'girl_22',
        'girl_23',
        'girl_24',
        'girl_25',
        'girl_26',
        'girl_27',
        'girl_28',
        'girl_29',
        'girl_30',
        'girl_31',
        'girl_32',
        'girl_33',
        'girl_34',
        'girl_35',
        'girl_36',
        'girl_37',
        'girl_38',
        'girl_39',
        'girl_40',
    ]

    fight_girl_dis_df = fight_girl_result_df[columns]
    #print fight_girl_dis_df
    del_sql = 'delete from {0} where ds="{1}"'.format(table, date)
    update_mysql(table, fight_girl_dis_df, del_sql)


if __name__ == '__main__':
    settings_dev.set_env('metal_test')
    dis_act_3day_fight_girl('20160524')
