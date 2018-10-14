#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Hu Wenchao
Description : 装备命运
'''
import settings_dev
import pandas as pd
from collections import defaultdict
from utils import ds_add, hql_to_df, update_mysql, get_config
from sanguo.cfg import card_equip_pos_map
from collections import Counter


def dis_chain(date):
    evo_range = range(51)  # 预留的显示进阶数范围
    # 卡牌上阵人数表
    table_card_shangzheng_num = 'dis_card_shangzheng_num'
    # 卡牌装备命运展示表
    table_card_equip_chain = 'dis_card_equip_chain'

    card_equip_chain_sql = '''
    select * from
    (
      select user_id, vip, card_dict, equip_dict
      from mid_info_all
      where ds = '{date}'
    ) t1
    left semi join
    (
      select distinct user_id
      from raw_activeuser
      where ds >= '{date_in_3days}' and ds <= '{date}'
    ) t2 on t1.user_id = t2.user_id
    '''.format(**{
        'date': date,
        'date_in_3days': ds_add(date, -2)
    })

    print card_equip_chain_sql
    card_equip_chain_df = hql_to_df(card_equip_chain_sql, 'hive')
    card_equip_chain_df.columns = ['user_id', 'vip', 'card_dict', 'equip_dict']
    print card_equip_chain_df.head()
    # card_equip_chain_df['card_dict'] = card_equip_chain_df['card_dict'].map(
    #     lambda s: eval(s))
    # card_equip_chain_df['equip_dict'] = card_equip_chain_df['equip_dict'].map(
    #     lambda s: eval(s))
    card_equip_chain_df.columns = ['user_id', 'vip', 'cards', 'equips']
    card_equip_chain_df['server'] = card_equip_chain_df['user_id'].map(
        lambda s: s[:-7])
    card_equip_chain_df['vip'] = card_equip_chain_df['vip'].map(
        lambda s: int(s))

    # 列出所有卡牌装备命运 id 对
    card_equip_chain_sets = set()
    chain_cfg = get_config('chain')
    equip_cfg = get_config('equip')
    character_detail_cfg = get_config('character_detail')

    for card_cid, card_info in character_detail_cfg.iteritems():
        chains = card_info['chain']
        character_id = card_info['character_ID']
        if card_info['is_notice'] < 3:
            continue
        for chain in chains:
            chain = str(chain)
            one_chain_cfg = chain_cfg[chain]
            if one_chain_cfg['condition_sort'] == '1':
                equip = one_chain_cfg['data'][0]
                one_equip_cfg = equip_cfg[str(equip)]
                equip_id = one_equip_cfg['equip_id']
                if one_equip_cfg['quality'] < 5:
                    continue
                # print character_id, equip_id, equip_cfg[str(equip)]['quality']
                card_equip_chain_sets.add((character_id, equip_id))

    # print len(card_equip_chain_sets)

    # 卡牌上阵人数，格式：(vip, server, character_id): num
    card_form_num_dic = defaultdict(int)

    def card_equip_chain_lines():
        for _, row in card_equip_chain_df.iterrows():
            # 记录各个位置的卡牌，{ 0: 'xxx0', 1: 'xxx1', ... , 8: 'xxx8' }
            pos_card_dic = {}
            for card_id, card_info in eval(row['cards']).iteritems():
                # 跳过没上阵的卡牌
                if card_info['pos'] not in card_equip_pos_map:
                    continue
                card_cfg_info = character_detail_cfg[str(card_info['c_id'])]
                if card_cfg_info['is_notice'] < 3:
                    continue
                pos_card_dic[card_equip_pos_map[card_info[
                    'pos']]] = card_cfg_info[
                        'character_ID']
                # 上阵卡牌数 +1
                card_form_num_dic[(row['vip'], row['server'], card_cfg_info[
                    'character_ID'])] += 1
            for equip_id, equip_info in eval(row['equips']).iteritems():
                # 位置上没有卡牌的，直接跳过
                if equip_info['pos'] not in pos_card_dic:
                    continue
                equip_cfg_info = equip_cfg[str(equip_info['c_id'])]
                if equip_cfg_info['quality'] < 5:
                    continue
                character_id = pos_card_dic[equip_info['pos']]
                equip_id = equip_cfg_info['equip_id']
                if (character_id, equip_id) not in card_equip_chain_sets:
                    continue
                yield [row.vip, row.server, character_id, equip_id,
                       equip_cfg_info['evo_num']]

    # 处理装备命运进阶数统计
    card_equip_chain_detail_df = pd.DataFrame(
        card_equip_chain_lines(),
        columns=['vip', 'server', 'character_id', 'equip_id', 'evo_num'])
    grouped_df = card_equip_chain_detail_df.groupby(
        ['vip', 'server', 'character_id', 'equip_id']).agg({
            'evo_num': lambda g: tuple(g)
        }).reset_index()
    grouped_df['evo_num_counter'] = grouped_df['evo_num'].map(
        lambda s: Counter(s))
    grouped_df['evo_num_all'] = grouped_df['evo_num'].map(lambda s: len(s))
    for evo in evo_range:
        grouped_df['evo_{0}'.format(evo)] = grouped_df['evo_num_counter'].map(
            lambda s: s.get(evo, 0))
    grouped_df['equip_name'] = grouped_df['equip_id'].map(
        lambda s: equip_cfg[str(s)]['name'])
    del grouped_df['evo_num_counter'], grouped_df['evo_num']
    # 加上日期，更新mysql
    grouped_df['ds'] = date
    del_sql = 'delete from {0} where ds="{1}"'.format(table_card_equip_chain,
                                                      date)
    update_mysql(table_card_equip_chain, grouped_df, del_sql)

    # 处理卡牌上阵人数
    shangzheng_card_num_df = pd.DataFrame(
        ([k[0], k[1], k[2], v] for k, v in card_form_num_dic.iteritems()),
        columns=['vip', 'server', 'character_id', 'shangzheng_num'])
    # 加上日期，更新mysql
    shangzheng_card_num_df['ds'] = date
    del_sql = 'delete from {0} where ds="{1}"'.format(
        table_card_shangzheng_num, date)
    update_mysql(table_card_shangzheng_num, shangzheng_card_num_df, del_sql)


if __name__ == '__main__':
    settings_dev.set_env('sanguo_ks')
    dis_chain('20160420')
    # settings_dev.set_env('sanguo_tx')
    # dis_act_3day('20160419')
