#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Zhang Yongchen
Description : 卡牌飞升（转生），字段evo,0-15转
Name        : dis_card_career_dancer
Original    : dis_card_career_dancer
'''
import settings_dev
import pandas as pd
from utils import hql_to_df, update_mysql, get_config, date_range, ds_add
from collections import Counter
from dancer.cfg import zichong_uids
import time
import datetime

zichong_list = str(tuple(zichong_uids))

def dis_card_career_dancer(date):

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

    table = 'dis_card_career_dancer'
    print table
    dis_card_career_sql = '''
    SELECT reverse(substr(reverse(user_id),8)) AS server,
           user_id,
           vip,
           card_dict
    FROM mid_info_all
    WHERE ds = '{date}'
      AND user_id NOT IN {zichong_list}
      AND regexp_replace(to_date(act_time), '-', '') >='{date_ago}'
    '''.format(date=date, date_ago=ds_add(date, -6), zichong_list=zichong_list)

    # print dis_card_career_sql

    dis_card_career_df = hql_to_df(dis_card_career_sql)

    info_config = get_config('character_info')
    detail_config = get_config('character_detail')

    card_pos = set(range(1, 10, 1))

    # 解析遍历card_dict日志文件
    def card_career_lines():
        for _, row in dis_card_career_df.iterrows():
            for cards_id, card_info in eval(row['card_dict']).iteritems():
                card_id = cards_id.split('-')[0]
                character_id = str(
                    detail_config.get(
                        card_id, {}).get('character_id'))
                c_name = info_config.get(character_id, {}).get('name')
                attend_num = card_info['pos'] in card_pos
                career_lv = card_info['evo'] if attend_num else -1
                yield [row.server, row.vip, row.user_id, character_id,
                       c_name, attend_num, career_lv]

    # 形成新的df文件
    card_info_df = pd.DataFrame(
        card_career_lines(),
        columns=['server', 'vip', 'user_id', 'character_id', 'c_name',
                 'attend_num', 'career_lv'])

    # 统计出场次数
    grouped_result_df0 = card_info_df[card_info_df['attend_num'] == 1].groupby(
        ['server', 'vip', 'character_id', 'c_name']).agg({
            'user_id': lambda g: g.nunique(),
        }).reset_index().rename(columns={'user_id': 'have_user_pos'})

    # 统计拥有人数
    grouped_result_df1 = card_info_df.groupby(
        ['server', 'vip', 'character_id', 'c_name']).agg({
            'user_id': lambda g: g.nunique(),
            'career_lv': lambda g: tuple(g),
        }).reset_index()
    # 统计各飞升等级对应的数量(列表形式存储)
    grouped_result_df1['career_lv_counter'] = grouped_result_df1.career_lv.map(
        lambda x: Counter(x))
    # 统计各飞升等级对应的数量（分列显示,剔除lv为-1,即剔除未上场的卡牌）
    for lv in range(16):
        grouped_result_df1['career_{0}'.format(lv)] = grouped_result_df1[
            'career_lv_counter'].map(lambda x: x.get(lv, 0))

    # 出场信息与总信息合并
    grouped_result_all_df = grouped_result_df1.merge(
        grouped_result_df0,
        on=['server', 'vip', 'character_id', 'c_name'])
    # 加入日期索引
    grouped_result_all_df['ds'] = date
    # 设置列名
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
        'user_id': 'have_user_num',
        'character_id': 'card_id',
        'have_user_pos': 'attend_num',
        'c_name': 'card_name',
        'vip': 'vip_level'
    })
    # print dis_card_career_df

    del_sql = 'delete from {0} where ds="{1}"'.format(table, date)
    update_mysql(table, dis_card_career_df, del_sql)
    return dis_card_career_df

if __name__ == '__main__':
    for platform in ('dancer_tw', 'dancer_pub'):
        settings_dev.set_env(platform)
    #     settings_dev.set_env('dancer_tw')
        dis_card_career_dancer('20170327')
