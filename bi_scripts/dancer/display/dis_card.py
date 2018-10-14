#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Dong Junshuang
Description : 卡牌 - 卡牌进阶 卡牌飞升 装备进阶
Time        : 2017.05.22
illustration: 每周跑数；近7天数据；
              卡牌进阶阶数和卡牌飞升等级数据为上阵卡牌的数据；
              装备进阶阶数为上阵装备的数据；
'''
import datetime
import settings_dev
import pandas as pd
from utils import ds_add
from utils import hql_to_df
from utils import date_range
from utils import get_config
from utils import update_mysql
from dancer.cfg import zichong_uids

equip_shangzhen_pos = set(range(9))  # 上阵装备位置
card_pos = set(range(1, 10, 1))  # 上阵卡牌的位置
career_dic = {num: 'career_%d' % num for num in range(31)}  # 卡牌飞升
orange_dic = {d: 'orange_%d' % d for d in range(4)}  # 卡牌进阶 - 橙卡
red_dic = {d: 'red_%d' % d for d in range(41)}  # 卡牌进阶 - 红卡
equip_dic = {d: 'red_%d' % d for d in range(21)}  # 装备进阶


def dis_card(date):

    if datetime.datetime.strptime(date, '%Y%m%d').weekday() != 6:
        print '{0} Is Not Sunday'.format(date)
        return

    card_sql = '''
    SELECT user_id,
           vip as vip_level,
           card_dict,
           equip_dict
    FROM
      (SELECT user_id,
              vip,
              card_dict,
              equip_dict,
              row_number() over(partition BY user_id
                                ORDER BY ds DESC) AS rn
       FROM parse_info
       WHERE ds >= '{date_ago}'
         AND ds<='{date}' )a
    WHERE rn =1
    '''.format(date_ago=ds_add(date, -6), date=date)
    card_df = hql_to_df(card_sql)
    # 排除测试用户
    card_df = card_df[~card_df['user_id'].isin(set(zichong_uids))]

    # 获取配置表
    info_config = get_config('character_info')
    detail_config = get_config('character_detail')
    # 获取装备配置表
    equip_info_config = get_config('equip_info')
    equip_detail_config = get_config('equip')

    def card_lines():
        for _, row in card_df.iterrows():
            for cards_id, card_info in eval(row['card_dict']).iteritems():
                c_id = str(card_info['c_id'])
                card_id = str(detail_config.get(c_id, {}).get('character_id'))
                # print cards_id.split('-')[0], character_id
                # print str(card_info['c_id'])
                card_name = info_config.get(card_id, {}).get('name')
                attend_num = card_info['pos'] in card_pos
                career_lv = card_info['evo'] if attend_num else -1
                quality = detail_config.get(c_id, {}).get('quality')
                step = detail_config.get(c_id, {}).get('step')
                yield [row.user_id, row.vip_level, card_id, card_name,
                       attend_num, career_lv, quality, step]

    # 生成卡牌的DataFrame
    column = ['user_id', 'vip_level', 'card_id', 'card_name', 'attend_num',
              'career_lv', 'quality', 'step']
    card_all_df = pd.DataFrame(card_lines(), columns=column)
    card_all_df['total_num'] = 1

    def equip_lines():
        for _, row in card_df.iterrows():
            for equips_id, equip_info in eval(row.equip_dict).iteritems():
                # print equip_id, equip_info
                # equip_id = str(equip_info['c_id'])
                equip_id = equips_id.split('-')[0]
                equip_name = equip_info_config.get(equip_id, {})['name']
                one_equip_cfg = equip_detail_config.get(
                    str(equip_info['c_id']), {})
                # evo_num 为阶数，由于只统计上阵的，没上阵的标记为 -1
                is_shangzhen = equip_info.get('pos', {}) in equip_shangzhen_pos
                evo_num = one_equip_cfg.get('step', {}) if is_shangzhen else -1
                yield [row.vip_level, row.user_id, equip_id, equip_name,
                       is_shangzhen, evo_num]
    # 生成装备的DataFrame
    equip_all_df = pd.DataFrame(equip_lines(),
                                columns=['vip_level', 'user_id', 'equip_id',
                                         'equip_name', 'is_shangzhen',
                                         'evo_num'])
    equip_all_df['total_num'] = 1
    # ============================================================
    # 卡牌汇总
    card_total_df = card_all_df.groupby(
        ['vip_level', 'card_id', 'card_name']).agg({
            'user_id': 'nunique',
            'attend_num': 'sum',
            'total_num': 'sum',
        }).reset_index().rename(columns={'user_id': 'have_user_num'})
    card_total_df['ds'] = date
    # ============================================================
    # 卡牌飞升 - career_lv为-1表示未上场的
    card_career_df = (
        card_all_df[(card_all_df.attend_num == 1) & (card_all_df.career_lv !=
                                                     -1)]
        .groupby(['vip_level', 'card_id', 'career_lv']).total_num.sum(
        ).reset_index()
        .pivot_table('total_num', ['vip_level', 'card_id'], 'career_lv')
        .reset_index().fillna(0).rename(columns=career_dic))
    # 补缺失数据
    for i in ['career_%d' % num for num in range(31)]:
        if i not in card_career_df.columns:
            card_career_df[i] = 0
    card_career_result = (card_total_df.merge(card_career_df,
                                              on=['vip_level', 'card_id'],
                                              how='outer').fillna(0))
    # 更新MySQL表
    career_column = ['ds', 'vip_level', 'card_id', 'card_name',
                     'have_user_num', 'attend_num', 'total_num'
                     ] + ['career_%d' % num for num in range(31)]
    card_career_table = 'dis_card_career_dancer'
    del_sql = 'delete from {0} where ds="{1}"'.format(card_career_table, date)
    update_mysql(card_career_table, card_career_result[career_column], del_sql)
    print '{0} complete'.format(card_career_table)
    # ============================================================
    # 卡牌进阶 - 4：紫色，5：橙色，6：红色
    card_evo_tatal = (
        card_all_df[(card_all_df.attend_num == 1) & (card_all_df.quality > 4)]
        .groupby(['vip_level', 'card_id', 'quality', 'step'])
        .total_num.sum().reset_index())
    orange_evo_df = (
        card_evo_tatal[card_evo_tatal.quality == 5]
        .pivot_table('total_num', ['vip_level', 'card_id'], 'step')
        .reset_index().rename(columns=orange_dic).fillna(0))
    red_evo_df = (card_evo_tatal[card_evo_tatal.quality == 6]
                  .pivot_table('total_num', ['vip_level', 'card_id'], 'step')
                  .reset_index().rename(columns=red_dic).fillna(0))
    # 补缺失数据
    for i in ['orange_%d' % d for d in range(4)]:
        if i not in orange_evo_df.columns:
            orange_evo_df[i] = 0
    for i in ['red_%d' % d for d in range(41)]:
        if i not in red_evo_df.columns:
            red_evo_df[i] = 0
    # 卡牌进阶数据
    card_evo_df = (card_total_df.merge(orange_evo_df,
                                       on=['vip_level', 'card_id'],
                                       how='outer')
                   .merge(red_evo_df,
                          on=['vip_level', 'card_id'],
                          how='outer').fillna(0))
    # 更新MySQL表
    evo_column = ['ds', 'vip_level', 'card_id', 'card_name', 'have_user_num',
                  'attend_num', 'total_num'] + [
                      'orange_%d' % num for num in range(4)
                  ] + ['red_%d' % num for num in range(41)]
    card_evo_table = 'dis_card_evo_dancer'
    del_sql = 'delete from {0} where ds="{1}"'.format(card_evo_table, date)
    update_mysql(card_evo_table, card_evo_df[evo_column], del_sql)
    print '{0} complete'.format(card_evo_table)
    # ============================================================
    # 装备汇总
    equip_total_df = equip_all_df.groupby(
        ['vip_level', 'equip_id', 'equip_name']).agg({
            'user_id': 'nunique',
            'is_shangzhen': 'sum',
            'total_num': 'sum',
        }).reset_index().rename(columns={'user_id': 'have_user_num',
                                         'is_shangzhen': 'attend_sum'})
    equip_total_df['ds'] = date
    # 装备进阶 - 只统计上阵卡牌的进阶阶数
    equip_df = (equip_all_df[equip_all_df.is_shangzhen == 1].groupby(
        ['vip_level', 'equip_id', 'evo_num']).total_num.sum()
                .reset_index().pivot_table(
                    'total_num', ['vip_level', 'equip_id'], 'evo_num')
                .reset_index().fillna(0).rename(columns=equip_dic))
    # 补缺失数据
    for i in ['red_%d' % d for d in range(21)]:
        if i not in equip_df.columns:
            equip_df[i] = 0
    equip_result = equip_total_df.merge(equip_df,
                                        on=['vip_level', 'equip_id'],
                                        how='outer').fillna(0)
    # 更新MySQL表
    equip_column = ['ds', 'vip_level', 'equip_id', 'equip_name',
                    'have_user_num', 'attend_sum', 'total_num'] + [
                        'red_%d' % d for d in range(21)
                    ]
    equip_table = 'dis_equip'
    del_sql = 'delete from {0} where ds="{1}"'.format(equip_table, date)
    update_mysql(equip_table, equip_result[equip_column], del_sql)
    print '{0} complete'.format(equip_table)


if __name__ == '__main__':
    for platform in ['dancer_bt', 'dancer_tw', 'dancer_pub', 'dancer_kr', 'dancer_mul']:
        settings_dev.set_env(platform)
        for date in date_range('20171217', '20171217'):
            print platform, date
            dis_card(date)
