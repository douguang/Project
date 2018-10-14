#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Dong Junshuang
Description : 卡牌 - 卡牌进阶 卡牌飞升 装备进阶- 突破等级
Time        : 2018.04.20
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


def dis_card():


    card_sql = '''
    SELECT user_id as user_id,
           vip as vip_level,
           card_dict as card_dict,
           money as money_today ,
           money_sum as money_sum
    FROM
      (
     
        select t1.user_id, t1.vip, t1.card_dict, t1.rn, t2.money, t3.money_sum
        from (
          SELECT user_id,
                      vip,
                      card_dict,
                      row_number() over(partition BY user_id
                                        ORDER BY ds DESC) AS rn
               FROM parse_info
               WHERE ds ='20180501' and user_id in(select user_id from mid_info_all where ds = '20180426' and vip >3 and vip<16)
               
          )t1 left join
          (      
        select user_id, order_time,  sum(order_money)as money from raw_paylog where to_date(order_time) = '2018-02-05' and user_id in (select user_id from mid_info_all where ds = '20180426' and vip >3 and vip<16) group by user_id, order_time
               
            )t2 on t1.user_id = t2.user_id left join
            (
              select user_id, order_time,  sum(order_money)as money_sum from raw_paylog where  to_date(order_time) >= '2018-02-05' and to_date(order_time) < '2018-02-16'  and user_id in (select user_id from mid_info_all where ds = '20180426' and vip >3 and vip<16) group by user_id, order_time
         
              )t3 on t2.user_id = t3.user_id
        
       )a
    WHERE rn =1
    '''
    card_sql2 = '''
        SELECT user_id,
                      vip,
                      card_dict
               FROM mid_info_all
               WHERE ds ='20180501' and vip <= 6 and vip >0
    '''
    print card_sql2
    card_df = hql_to_df(card_sql2)
    # 排除测试用户
    card_df = card_df[~card_df['user_id'].isin(set(zichong_uids))]

    # 获取配置表
    info_config = get_config('character_info')
    detail_config = get_config('character_detail')
    # print    card_df
    # 获取装备配置表
    # equip_info_config = get_config('equip_info')
    # equip_detail_config = get_config('equip')

    def card_lines():
        for _, row in card_df.iterrows():
            for cards_id, card_info in eval(row['card_dict']).iteritems():
                # print    card_info
                c_id = str(card_info['c_id'])
                if c_id in ['98600']:
                    card_id = str(detail_config.get(c_id, {}).get('character_id'))
                    card_name = info_config.get(card_id, {}).get('name')
                    attend_num = card_info['pos'] in card_pos
                    career_lv = card_info['evo']
                    quality = detail_config.get(c_id, {}).get('quality')
                    step = detail_config.get(c_id, {}).get('step')
                    print 'jjjjjjjjjjjjjjjjjjjjj'
                    print card_info
                    print 'evo = =======  ='
                    print card_info['evo']
                    breakthrough_ids = card_info['breakthrough']
                    # if card_id >= '80' and card_id < '99':

                    yield [row.user_id, row.vip, card_id, card_name, attend_num, career_lv, quality, step, breakthrough_ids]
                    # yield [row.user_id, row.vip_level, card_id, card_name,
                    #      attend_num, career_lv, quality, step,row.money_today,row.money_sum, breakthrough_ids] #, breakthrough_ids

    # 生成卡牌的DataFrame
    column = ['user_id', 'vip', 'card_id', 'card_name', 'attend_num', 'career_lv', 'quality', 'step', 'breakthrough_ids']
    card_all_df = pd.DataFrame(card_lines(), columns=column)
    card_all_df['total_num'] = 1
    card_all_df.to_excel('/Users/kaiqigu/Desktop/BI_excel/20180504/dancer_card_20180504_1.xlsx')
    # print card_all_df


if __name__ == '__main__':
    for platform in ['dancer_pub']:
        settings_dev.set_env(platform)
        dis_card()
