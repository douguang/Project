#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
@Time    : 16-12-8 上午11:18
@Author  : Andy 
@File    : card_evo_career_reincarnation_espirit_.py
@Software: PyCharm
Description :
进阶evo_level,转职career,转生reincarnation,器灵unique_espirit
'''

import settings_dev
import pandas as pd
from utils import hql_to_df,ds_add, get_config, date_range
from pandas import DataFrame

def card_career_user_info(start_ds,end_ds):
    card_sql = '''
      select
        t1.user_id,level,vip,sum_money,card_dict
      from
      (
          select
            user_id,level,vip,card_dict
          from
            mid_info_all
          where
            ds='{end_ds}'
            and act_time >= '2017-04-13 00:00:00'
      ) t1
      left join
      (
          select
            sum(order_money) as sum_money,user_id
          from
            raw_paylog
          where
            ds>='{start_ds}'
          group by
            user_id
      ) t2
      on
        t1.user_id=t2.user_id
    '''.format(start_ds=start_ds,end_ds=end_ds)
    print card_sql
    card_df = hql_to_df(card_sql)

    detail_config = get_config('character_detail')

    card_pos = set(range(1, 10, 1))

    def card_evo_lines():
        for _, row in card_df.iterrows():
            for card_id, card_info in eval(row['card_dict']).iteritems():
                # print card_info
                c_id = card_id.split('-')[0]
                character_id = detail_config.get(c_id, {}).get(
                    'character_ID', '-99')
                #if character_id in card_list:
                if 'unique_espirit' in card_info and card_info['unique_espirit'] != 0:
                    c_name = detail_config.get(c_id, {}).get('name')
                    attend_num = card_info['pos'] in card_pos
                    quality = detail_config.get(c_id, {}).get('quality')
                    career = card_info['career']
                    unique_espirit = ''
                    reincarnation = ''
                    try:
                        unique_espirit = card_info['unique_espirit']
                        reincarnation = card_info['reincarnation']
                    except:
                        pass
                    step = detail_config.get(c_id, {}).get('evo_level')
                    yield [row.user_id, row.level, row.vip, character_id, c_name, attend_num, quality, step, career, reincarnation, unique_espirit, c_id]

    card_info_df = pd.DataFrame(card_evo_lines(), columns=[
        'user_id', 'level', 'vip', 'character_id', 'c_name', 'attend_num', 'quality', 'step', 'career', 'reincarnation', 'unique_espirit', 'c_id'])
    print card_info_df
    #id 等级 vip 卡牌 名字 上阵 品质 进阶 专职 转世 器灵 卡牌ID
    rename_dic = {'user_id': '用户id',
                  'level': '等级',
                  'vip': '用户VIP',
                  'character_id': '卡牌',
                  'c_name': '名字',
                  'attend_num': '是否上阵',
                  'quality': '品质',
                  'step': '进阶',
                  'career': '专职',
                  'reincarnation': '转世',
                  'unique_espirit': '器灵',
                  'c_id': '卡牌id',}
    card_info_df = card_info_df.rename(columns=rename_dic)
    return card_info_df


if __name__ == '__main__':
    #   周瑜7001,司马懿7002,诸葛亮7004,郭嘉7005,吕布7008,赵云7009,陆逊6002,貂蝉6003,张辽7003,
    #   关羽7006,孙策7007,曹操6001,张飞6004,贾诩6005,黄忠6006,夏侯惇6007,马超6009,甘宁6010,吕蒙5002,
    #   典韦5003,孙权5006,刘备5007,太史慈5009,夏侯渊5010,华佗4028,荀彧4019,徐庶4021,庞统4020,鲁肃4024,
    #   孙坚5014,黄月英5008,小乔5013,主角9001,左慈7014,于吉7013,卑弥呼7017
    game_verson=['sanguo_ks',]
    start_ds = '20170413'
    end_ds = '20170613'
    #
    #card_list = ['7017',]
    for i in game_verson:
        settings_dev.set_env(i)
        #result = card_career_user_info(start_ds,end_ds,card_list)
        result = card_career_user_info(start_ds,end_ds,)
        print result.head(5)
        result.to_excel('/home/kaiqigu/桌面/机甲无双_%s_%s-%s_器灵数据需求.xlsx' % (i,start_ds,end_ds) , index=False)
