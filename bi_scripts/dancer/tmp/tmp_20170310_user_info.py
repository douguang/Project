#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Zhang Yongchen
Description : 玩家  按服务器开服天数（或注册天数） 战力 UR平均进阶（上阵）UR平均飞升（上阵）ssr装备平均进阶（上阵）UR装备平均进阶（上阵）元宝存量 元宝消费 充值金额 服务器收入
'''

import settings_dev
from utils import hql_to_df, get_config
import pandas as pd

def tmp_20170310_user_info():

    #常规信息
    common_sql = '''
        select t1.user_id, t1.server, t1.regtime, t1.combat, t1.coin, t2.pay, t3.spend, t4.server_pay, t1.ds from
        (select user_id, reverse(substr(reverse(user_id) ,8)) as server, regexp_replace(to_date(reg_time),'-','') as regtime, combat, (free_coin + charge_coin) as coin, ds from parse_info where ds>='20161110' and user_id in ('g613369261', 'g250208955', 'g228587760', 'g20114436', 'g156594013')) t1
        left join
        (select user_id, sum(order_money) as pay, ds from raw_paylog where  ds>='20161110' and user_id in ('g613369261', 'g250208955', 'g228587760', 'g20114436', 'g156594013') and platform_2<>'admin_test' group by user_id, ds) t2
        on (t1.user_id=t2.user_id and t1.ds=t2.ds) left join
        (select user_id, sum(coin_num) as spend, ds from raw_spendlog where ds>='20161110' and user_id in ('g613369261', 'g250208955', 'g228587760', 'g20114436', 'g156594013') group by user_id, ds) t3
        on (t1.user_id=t3.user_id and t1.ds=t3.ds) left join
        (select reverse(substr(reverse(user_id) ,8)) as server, sum(order_money) as server_pay, ds from raw_paylog where  ds>='20161110' and  platform_2<>'admin_test' group by server, ds) t4
        on (t1.server=t4.server and t1.ds=t4.ds)
    '''
    print common_sql
    common_df = hql_to_df(common_sql)
    # print common_df.head(10)

    #卡牌
    card_sql = '''
        select user_id, card_dict, equip_dict, ds from parse_info where ds>='20161110' and user_id in ('g613369261', 'g250208955', 'g228587760', 'g20114436', 'g156594013')
    '''
    print card_sql
    card_df = hql_to_df(card_sql)
    # print card_df.head(10)

    detail_config = get_config('character_detail')
    card_pos = set(range(1, 10, 1))

    def card_evo_lines():
        for _, row in card_df.iterrows():
            for cards_id, card_info in eval(row['card_dict']).iteritems():
                card_id = str(card_info['c_id'])
                attend_num = card_info['pos'] in card_pos
                if attend_num:
                    quality = detail_config.get(card_id, {}).get('quality')
                    step = detail_config.get(card_id, {}).get('step')
                    evo = card_info['evo']
                    yield [row.user_id, quality, step, evo, row.ds]
    card_info_df = pd.DataFrame(card_evo_lines(), columns=[ 'user_id', 'quality', 'step', 'evo', 'ds'])
    card_info_df = card_info_df[card_info_df['quality'] == 6]
    card_info_df = card_info_df.groupby(['user_id', 'ds']).agg({'step': lambda g: g.sum()*1.0/g.count(), 'evo': lambda g: g.sum()*1.0/g.count()}).rename(columns={'step': 'card_step', 'evo': 'card_evo'}).reset_index()
    # print card_info_df

    # 装备
    detail_config = get_config('equip')
    equip_shangzheng_pos = set(range(9))

    def equip_lines():
        for _, row in card_df.iterrows():
            for equip_id, equip_info in eval(row.equip_dict).iteritems():
                c_id = str(equip_info['c_id'])
                one_equip_cfg = detail_config[str(c_id)]
                is_shangzhen = equip_info['pos'] in equip_shangzheng_pos
                if is_shangzhen:
                    quality = one_equip_cfg['quality']
                    step = one_equip_cfg['step'] if is_shangzhen else -1
                    yield [row.user_id, quality, step, row.ds]

    equip_df = pd.DataFrame(equip_lines(), columns=['user_id', 'quality', 'step', 'ds'])

    equip_df_5 = equip_df[equip_df['quality'] == 5]
    equip_df_5 = equip_df_5.groupby(['user_id', 'ds']).agg({'step': lambda g: g.sum()*1.0/g.count()}).rename(columns={'step': 'ssr_step'}).reset_index()

    equip_df_6 = equip_df[equip_df['quality'] == 6]
    equip_df_6 = equip_df_6.groupby(['user_id', 'ds']).agg({'step': lambda g: g.sum()*1.0/g.count()}).rename(columns={'step': 'ur_step'}).reset_index()

    #合并
    equip_all_df = equip_df_5.merge(equip_df_6, on=['user_id', 'ds'], how='left').fillna(0)
    card_equip_df = card_info_df.merge(equip_all_df, on=['user_id', 'ds'], how='outer').fillna(0)
    result_df = common_df.merge(card_equip_df, on=['user_id', 'ds'], how='left').fillna(0)

    print result_df.head(20)
    return result_df

if __name__ == '__main__':

    settings_dev.set_env('dancer_pub')
    result = tmp_20170310_user_info()
    result.to_excel(r'E:\Data\output\dancer\user_info.xlsx')