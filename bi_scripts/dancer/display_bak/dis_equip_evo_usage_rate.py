#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Hu Wenchao,Zhang Yongchen
Description : 装备概况
# TODO:
        修改：不再分品质统计装备信息，所有装备按进阶数统计，mysql中无用数据列全部赋值为0,无需修改mysql表结构。
'''
import pandas as pd
from utils import hql_to_df, update_mysql, ds_add, get_config, date_range
from collections import Counter
import settings_dev
from dancer.cfg import zichong_uids
import time
import datetime

zichong_uids = str(tuple(zichong_uids))

def dis_equip_evo_usage_rate(date):

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

    table = 'dis_equip'
    print date, table

    equip_sql = '''
        select vip, user_id, equip_dict
        from mid_info_all
        where ds = '{date}' and user_id not in {zichong_uids} and regexp_replace(to_date(act_time), '-', '') >='{date_ago}'
    '''.format(**{
        'date': date, 'zichong_uids': zichong_uids, 'date_ago': ds_add(date, -6)
    })

    # print equip_sql

    # 获取装备配置
    # equip_cfg = get_config('equip')
    info_config = get_config('equip_info')
    detail_config = get_config('equip')

    equip_df = hql_to_df(equip_sql)
    equip_df.columns = ['vip', 'user_id', 'equip_dict']
    # equip_df['equip_dict'] = equip_df['equip_dict'].map(lambda s: eval(s))
    equip_df['server'] = equip_df['user_id'].map(lambda s: s[:-7])

    # 上阵装备位置
    equip_shangzheng_pos = set(range(9))

    # 把equip_dict 展开，每一个装备合并其它几个数据变为一行
    def equip_lines():
        for _, row in equip_df.iterrows():
            for equip_id, equip_info in eval(row.equip_dict).iteritems():
                c_id = str(equip_info['c_id'])
                # character_id = detail_config.get(c_id, {}).get('equip_id')
                one_equip_cfg = detail_config[str(c_id)]
                # if one_equip_cfg['quality'] >= 5:
                # evo_num 为阶数，由于只统计上阵的，没上阵的标记为 -1
                is_shangzhen = equip_info['pos'] in equip_shangzheng_pos
                evo_num = one_equip_cfg['step'] if is_shangzhen else -1
                yield [row.vip, row.server, row.user_id, c_id, is_shangzhen, evo_num]

    equip_all_df = pd.DataFrame(equip_lines(), columns=['vip', 'server', 'user_id', 'c_id', 'is_shangzhen', 'evo_num'])
    # 索引装备唯一id，分组统计
    # equip_all_df['equip_id'] = equip_all_df.c_id.map(
    #     lambda c_id: info_config[str(c_id)]['equip_id'])
    equip_all_df['equip_id'] = equip_all_df.c_id.map(lambda c_id: detail_config[str(c_id)]['equip_id'])

    grouped_result_df = equip_all_df.groupby(
        ['vip', 'server', 'equip_id']).agg({
            'user_id': lambda g: g.nunique(),  # 拥有人数
            'c_id': lambda g: g.count(),  # 拥有数量
            'is_shangzhen': lambda g: g.sum(),  # 上阵数量
            'evo_num': lambda g: tuple(g),  # 阶数展成tuple，之后统计每阶数量
        }).reset_index()

    # 最后索引名字加进去
    grouped_result_df['name'] = grouped_result_df['equip_id'].map(
        lambda x: info_config[str(x)]['name'])
    # 统计各阶装备数量
    grouped_result_df['evo_num_counter'] = grouped_result_df.evo_num.map(
        lambda x: Counter(x))
    for evo in range(21):
        grouped_result_df['red_{0}'.format(evo)] = grouped_result_df[
            'evo_num_counter'].map(lambda s: s.get(evo, 0))
    final_df = grouped_result_df

    # 加入日期列，根据最后展示顺序重新排序列
    final_df['attend_rate'] = final_df['is_shangzhen']*1.0/final_df['c_id']
    final_df['ds'] = date
    final_df['org_attend_player_num'] = 0.0
    final_df['red_attend_player_num'] = 0.0
    final_df['org_all_num'] = 0.0
    final_df['red_all_num'] = 0.0

    columns = [
        'server',
        'vip',
        'equip_id',
        'name',
        'c_id',
        'user_id',
        'is_shangzhen',
        'attend_rate',
        'org_attend_player_num',
        'red_attend_player_num',
        'org_all_num',
        'red_all_num',
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
        'ds'
    ]
    final_dis_df = final_df[columns].rename(
        columns={'user_id': 'have_user_num',
                 'c_id': 'all_num',
                 'is_shangzhen': 'attend_sum',
                 'name': 'equip_name'})
    # print final_dis_df.head(20)
    #final_dis_df.to_excel('/home/kaiqigu/Documents/equip.xlsx')

    # 更新MySQL表
    del_sql = 'delete from {0} where ds="{1}"'.format(table, date)
    update_mysql(table, final_dis_df, del_sql)

if __name__ == '__main__':
    for platform in ['dancer_tw', 'dancer_pub']:
        print platform
        settings_dev.set_env(platform)
        dis_equip_evo_usage_rate('20170327')
