#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Zhang Yongchen
Description : 奖励中心获取元宝数
'''
from utils import hql_to_df, ds_add, update_mysql, format_dates, date_range
import pandas as pd
import settings_dev
import types

def tmp_20170410_last_action(date):

    reward_sql = '''
        select user_id, freemoney_diff, ds from parse_actionlog where ds='{date}' and freemoney_diff>0 and a_typ like '%reward_center%'
    '''.format(date=date)
    print reward_sql
    reward_df = hql_to_df(reward_sql)
    print reward_df.head(15)
    result = reward_df.groupby(['user_id', 'ds']).freemoney_diff.sum().reset_index().rename(columns={'freemoney_diff': 'coin'})
    print result.head(10)
    return result

if __name__ == '__main__':
    settings_dev.set_env('dancer_pub')
    result_list = []
    for date in date_range('20170301', '20170409'):
        # tmp_20170410_last_action(date).to_excel(r'E:\Data\output\dancer\last_action_%s.xlsx'%date)
        result_list.append(tmp_20170410_last_action(date))
    result = pd.concat(result_list)
    result.to_excel(r'E:\Data\output\dancer\reward_center_coin.xlsx')
