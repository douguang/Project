#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Hu Wenchao
Description : 装备概况
# TODO: 1. 确认进阶数是上阵还是拥有
        2. 确定是否是根据equip_id聚合

        近7日的活跃用户的装备数据
'''
import pandas as pd
from utils import hql_to_df, update_mysql, ds_add, get_config, date_range
from collections import Counter
import settings_dev


def dis_equip(date):
    table = 'dis_equip'

    date_in_3days = ds_add(date, -2)

    equip_sql = '''
    select * from
    (
        select vip, user_id, equip_dict
        from mid_info_all
        where ds = '{date}'
        and user_id in(
          select user_id
          from parse_actionlog
          where ds>='{date_in_3days}'
          and ds<='{date}'
          group by user_id
        )
    ) t1
    left semi join
    (
        select distinct user_id
        from raw_activeuser
        where ds >= '{date_in_3days}' and ds <= '{date}'
    ) t2 on t1.user_id = t2.user_id
    '''.format(**{
        'date': date,
        'date_in_3days': date_in_3days,
    })

    print equip_sql

    # 获取装备配置
    equip_cfg = get_config('equip')

    equip_df = hql_to_df(equip_sql)
    equip_df.columns = ['vip', 'user_id', 'equip_dict']
    # equip_df['equip_dict'] = equip_df['equip_dict'].map(lambda s: eval(s))
    equip_df['server'] = equip_df['user_id'].map(lambda s: s[:-7])

    # 三日活跃人数，最后合并到表里
    dau_in_3day_df = equip_df.groupby(['vip', 'server']).count().reset_index()[
        ['vip', 'server', 'user_id']].rename(
            columns={'user_id': 'dau_in_3day'})

    # 上阵装备位置
    equip_shangzheng_pos = set(range(9))

    # 把equip_dict 展开，每一个装备合并其它几个数据变为一行
    def equip_lines():
        for _, row in equip_df.iterrows():
            for equip_id, equip_info in eval(row.equip_dict).iteritems():
                c_id = equip_info['c_id']
                one_equip_cfg = equip_cfg[str(equip_info['c_id'])]
                if one_equip_cfg['quality'] >= 5:
                    # evo_num 为阶数，由于只统计上阵的，没上阵的标记为 -1
                    is_shangzheng = equip_info['pos'] in equip_shangzheng_pos
                    evo_num = one_equip_cfg['evo_num'] if is_shangzheng else -1
                    yield [row.vip, row.server, row.user_id, c_id,
                           is_shangzheng, evo_num]

    equip_all_df = pd.DataFrame(equip_lines(),
                                columns=['vip', 'server', 'user_id', 'c_id',
                                         'is_shangzheng', 'evo_num'])
    # 索引装备唯一id，分组统计
    equip_all_df['equip_id'] = equip_all_df.c_id.map(
        lambda c_id: equip_cfg[str(c_id)]['equip_id'])
    # 分组统计
    grouped_result_df = equip_all_df.groupby(
        ['vip', 'server', 'equip_id']).agg({
            'user_id': lambda g: g.nunique(),  # 拥有人数
            'c_id': lambda g: g.count(),  # 拥有数量
            'is_shangzheng': lambda g: g.sum(),  # 上阵数量
            'evo_num': lambda g: tuple(g),  # 阶数展成tuple，之后统计每阶数量
        }).reset_index()

    # 最后索引名字加进去
    grouped_result_df['name'] = grouped_result_df['equip_id'].map(
        lambda x: equip_cfg[str(x)]['name'])
    # 统计各阶装备数量
    grouped_result_df['evo_num_counter'] = grouped_result_df.evo_num.map(
        lambda x: Counter(x))
    for evo in range(51):
        grouped_result_df['evo_{0}'.format(evo)] = grouped_result_df[
            'evo_num_counter'].map(lambda s: s.get(evo, 0))
    final_df = grouped_result_df.merge(dau_in_3day_df, on=['vip', 'server'])

    # 加入日期列，根据最后展示顺序重新排序列
    final_df['ds'] = date
    columns = [
        'ds',
        'vip',
        'server',
        'dau_in_3day',
        'equip_id',
        'name',
        'user_id',
        'c_id',
        'is_shangzheng',
        'evo_0',
        'evo_1',
        'evo_2',
        'evo_3',
        'evo_4',
        'evo_5',
        'evo_6',
        'evo_7',
        'evo_8',
        'evo_9',
        'evo_10',
        'evo_11',
        'evo_12',
        'evo_13',
        'evo_14',
        'evo_15',
        'evo_16',
        'evo_17',
        'evo_18',
        'evo_19',
        'evo_20',
        'evo_21',
        'evo_22',
        'evo_23',
        'evo_24',
        'evo_25',
        'evo_26',
        'evo_27',
        'evo_28',
        'evo_29',
        'evo_30',
        'evo_31',
        'evo_32',
        'evo_33',
        'evo_34',
        'evo_35',
        'evo_36',
        'evo_37',
        'evo_38',
        'evo_39',
        'evo_40',
        'evo_41',
        'evo_42',
        'evo_43',
        'evo_44',
        'evo_45',
        'evo_46',
        'evo_47',
        'evo_48',
        'evo_49',
        'evo_50',
    ]
    final_dis_df = final_df[columns].rename(
        columns={'user_id': 'have_user_num',
                 'c_id': 'have_num',
                 'is_shangzheng': 'shangzheng_num'})
    # print final_dis_df

    # 更新MySQL表
    del_sql = 'delete from {0} where ds="{1}"'.format(table, date)
    update_mysql(table, final_dis_df, del_sql)


if __name__ == '__main__':
    for platform in ['sanguo_tw', 'sanguo_ks', 'sanguo_tl', 'sanguo_tx', 'sanguo_ios', 'sanguo_kr', ]:  # 腾讯 dancer_tx  国内 dancer_pub
        settings_dev.set_env(platform)
        dates = date_range('20170301', '20170313')
        for date in dates:
            dis_equip(date)

    print "end"
