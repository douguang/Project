#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Zhang Yongchen
Description : 卡牌使用率(武娘)
Database    : dancer_ks
'''
import settings_dev
import pandas as pd
from utils import ds_add, hql_to_df, update_mysql, get_config, date_range
from dancer.cfg import zichong_uids
import time
import datetime

zichong_uids = str(tuple(zichong_uids))

def dis_card_use_rate(date):

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

    table = 'dis_card_use_rate'
    print date,table
    sql = '''
    SELECT user_id,
           vip as vip_level,
           card_dict
    FROM mid_info_all
    WHERE ds = '{date}' and user_id not in {zichong_uids} and regexp_replace(to_date(act_time), '-', '') >='{date_ago}'
    '''.format(**{
        'date': date, 'zichong_uids': zichong_uids, 'date_ago': ds_add(date, -6)
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

    card_info_df = pd.DataFrame(card_career_lines(),
                                columns=['user_id', 'vip_level', 'character_id', 'card_name', 'attend_num'])
    use_df = card_info_df[card_info_df['attend_num'] == 1].groupby(
        ['character_id', 'card_name', 'vip_level']).count().reset_index().drop('user_id', axis=1)
    all_df = card_info_df.groupby(['character_id', 'card_name', 'vip_level']).agg({'user_id': lambda g: g.nunique(),
                                                                                   'attend_num': lambda g: g.count(),
                                                                                   }).reset_index()
    all_df['ds'] = date
    all_df = all_df.rename(columns={
        'user_id': 'have_user_num',
        'attend_num': 'total_num'
    })
    result_df = all_df.merge(use_df, on=['character_id', 'card_name', 'vip_level'], how='left')
    columns = ['ds', 'vip_level', 'character_id', 'card_name', 'have_user_num', 'total_num',
               'attend_num']  # 去掉use_rate字段，在前端计算
    result_df = result_df[columns].sort_values('attend_num', ascending=False).fillna(0)
    result_df = result_df.rename(columns={'character_id':'card_id'})
    # print result_df
    #更新MySQL

    del_sql = 'delete from {0} where ds="{1}"'.format(table, date)
    update_mysql(table, result_df, del_sql)
    return result_df

if __name__ == '__main__':
    for platform in ('dancer_tw', 'dancer_pub'):
        settings_dev.set_env(platform)
        # for date in date_range('20161110', '20161121'):
        #         dis_card_career_dancer(date)
        dis_card_use_rate('20170327')
