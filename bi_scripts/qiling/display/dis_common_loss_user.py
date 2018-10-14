#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Dong Junshuang
Description : 流失用户分布 , 流失用户VIP分布
'''
import settings_dev
from utils import hql_to_df, update_mysql
import pandas as pd
import numpy as np


def dis_common_loss_user(date):
    lost_sql = '''
    SELECT user_id,
           vip,
           reg_time,
           act_time
    FROM mid_info_all
    WHERE ds = '{date}'
    '''.format(date=date)
    lost_df = hql_to_df(lost_sql)
    today = pd.Period(date)

    lost_df['lost_days'] = lost_df.act_time.map(
        lambda s: today - pd.Period(s[:10].replace('-', '')))
    ranges = [2, 3, 7, 15, 30, 5000]
    lost_df['lost_days'] = pd.cut(lost_df.lost_days, ranges).astype('object')
    lost_df['num'] = 1
    columns_to_rename = {
        '(15, 30]': 'lost_16_30',
        '(2, 3]': 'lost_3',
        '(3, 7]': 'lost_4_7',
        '(7, 15]': 'lost_8_15',
        '(30, 5000]': 'lost_31',
    }
    columns_to_show = ['vip', 'lost_3', 'lost_4_7', 'lost_8_15', 'lost_16_30',
                       'lost_31']
    lost_days_df = pd.pivot_table(
        lost_df, index=['vip'], columns=['lost_days'], values='num', aggfunc=np.sum, fill_value=0).reset_index().rename(columns=columns_to_rename)[columns_to_show]
    lost_days_df['ds'] = date

    # 更新MySQL表
    table = 'dis_common_loss_user'
    print table
    del_sql = 'delete from {0} where ds = "{1}"'.format(table, date)
    update_mysql(table, lost_days_df, del_sql)


if __name__ == '__main__':
    settings_dev.set_env('qiling_ks')
    date = '20170208'
    result = dis_common_loss_user(date)
