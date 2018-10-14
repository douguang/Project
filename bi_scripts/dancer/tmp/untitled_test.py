#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
用户样本取 8 、 9、10日新增用户且近3日活跃的用户
id  名字   付费金额   卡牌id   卡牌等级   卡牌进阶等级   卡牌飞升等级
'''
from utils import hql_to_df
import settings_dev


def reg_card_equip():
    card_sql = '''
    select *
    from parse_info
    limit 10
    '''
    print card_sql
    card_df = hql_to_df(card_sql)
    print card_df
    # card_df.to_excel(r'E:\My_Data_Library\dancer\2016-10-20\item_09.xlsx')

if __name__ == '__main__':
    settings_dev.set_env('dancer_tw')
    reg_card_equip()
