#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Zhang Yongchen
Description : 卡牌进阶(武娘),字段step,紫色（0-3阶），橙色（0-3阶），红色（0-40阶），分服务器，分vip，分日期。
'''
import settings_dev
import pandas as pd
from utils import ds_add, hql_to_df, update_mysql, get_config, date_range
from pandas import DataFrame
from dancer.cfg import zichong_uids
import time
import datetime

zichong_uids = str(tuple(zichong_uids))


def dis_card_evo_dancer(date):

    date = date[:4] + '-' + date[4:6] + '-' + date[6:8]
    # print date
    date = time.strptime(date, "%Y-%m-%d")
    # print date
    weeks_now = int(time.strftime('%w', date))
    # print weeks_now
    if weeks_now != 0:
        return
    else:
        date = str(datetime.date(*date[:3])).replace('-', '')

    table = 'dis_card_evo_dancer'
    print date, table
    card_sql = '''
    SELECT reverse(substr(reverse(user_id),8)) AS server,
           user_id,
           vip,
           card_dict
    FROM mid_info_all
    WHERE ds = '{date}' and user_id not in {zichong_uids} and regexp_replace(to_date(act_time), '-', '') >='{date_ago}'
    '''.format(date=date,
               date_ago=ds_add(date, -6),
               zichong_uids=zichong_uids)
    card_df = hql_to_df(card_sql)

    info_config = get_config('character_info')
    detail_config = get_config('character_detail')

    card_pos = set(range(1, 10, 1))

    def card_evo_lines():
        for _, row in card_df.iterrows():
            for cards_id, card_info in eval(row['card_dict']).iteritems():
                card_id = str(card_info['c_id'])
                character_id = str(detail_config.get(card_id, {}).get(
                    'character_id'))
                card_name = info_config.get(character_id, {}).get('name')
                attend_num = card_info['pos'] in card_pos
                quality = detail_config.get(card_id, {}).get('quality')
                step = detail_config.get(card_id, {}).get('step')
                yield [row.server, row.user_id, row.vip, character_id,
                       card_name, attend_num, quality, step, card_id]

    card_info_df = pd.DataFrame(card_evo_lines(),
                                columns=[
                                    'server', 'user_id', 'vip', 'character_id',
                                    'card_name', 'attend_num', 'quality',
                                    'step', 'card_id'
                                ])
    use_df = card_info_df[card_info_df['attend_num'] == 1]
    use_df = use_df[use_df['quality'] > 3]
    use_df['num'] = 1
    df0 = DataFrame()
    df0['step'] = range(41)
    df0['quality'] = 6
    df1 = DataFrame()
    df1['step'] = range(4)
    df1['quality'] = 5
    df2 = DataFrame()
    df2['step'] = range(4)
    df2['quality'] = 4
    df = pd.concat([df0, df1, df2])

    ori_df = use_df.merge(df, on=['quality', 'step'], how='outer').fillna(0)
    mid_df = pd.pivot_table(ori_df,
                            index=[
                                'server', 'vip', 'character_id', 'card_name'
                            ],
                            columns=[
                                'quality', 'step'
                            ],
                            aggfunc={
                                'num': sum
                            },
                            fill_value=0).reset_index()

    mid_df.columns = ['server', 'vip', 'character_id', 'card_name'] + [
        'purple_%d' % d for d in range(4)
    ] + ['orange_%d' % d for d in range(4)] + ['red_%r' % r for r in range(41)]
    mid_df = mid_df[mid_df['server'] != 0]
    # 卡牌拥有人数
    own_df = card_info_df.groupby(
        ['server', 'vip', 'character_id', 'card_name']).agg(
            {'user_id': lambda g: g.nunique()}).reset_index()
    # 卡牌上阵人数
    attend_df = use_df.groupby(
        ['server', 'vip', 'character_id', 'card_name']).agg(
            {'num': lambda g: g.sum()}).reset_index()
    all_df = attend_df.merge(own_df,
                             on=['server', 'vip', 'character_id', 'card_name'],
                             how='left')
    result_df = mid_df.merge(all_df,
                             on=['server', 'vip', 'character_id', 'card_name'],
                             how='left')
    result_df['ds'] = date
    columns = ['ds', 'server', 'vip', 'character_id', 'card_name', 'user_id',
               'num'] + ['purple_%d' % d
                         for d in range(4)] + ['orange_%d' % d for d in range(
                             4)] + ['red_%r' % r for r in range(41)]
    result_df = result_df[columns].rename(columns={
        'vip': 'vip_level',
        'character_id': 'card_id',
        'user_id': 'have_user_num',
        'num': 'attend_num',
    })
    # print result_df

    del_sql = 'delete from {0} where ds="{1}"'.format(table, date)
    update_mysql(table, result_df, del_sql)

    return result_df


if __name__ == '__main__':
    for platform in ('dancer_tw', 'dancer_pub'):
        settings_dev.set_env(platform)
        #     settings_dev.set_env('dancer_pub')
        dis_card_evo_dancer('20170327')
