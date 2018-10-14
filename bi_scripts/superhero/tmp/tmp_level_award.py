#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Dong Junshuang
Description : 等级礼包
'''
import settings_dev
from utils import hql_to_df
import pandas as pd

if __name__ == '__main__':
    settings_dev.set_env('superhero_tw')
    # date = '20161009'
    award_sql = '''
    SELECT ds,
           uid,
           args
    FROM raw_action_log
    WHERE ds >='20170315'
      AND ds <='20170319'
      AND action = 'user.level_award'
    '''
    award_df = hql_to_df(award_sql)

    award_df['lv'] = award_df['args'].map(lambda s: eval(s)['lv'][0])
    result_df = award_df.groupby(['ds','lv']).count().uid.reset_index()

    result_df.to_excel('/Users/kaiqigu/Documents/Excel/level_award.xlsx')
