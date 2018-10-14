#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Dong Junshuang
Description : 流失用户分布 , 流失用户VIP分布
'''
import settings_dev
from utils import hql_to_df, update_mysql,date_range
import pandas as pd
import numpy as np
from pandas import DataFrame


def dis_common_loss_user(date):
    print 'start'
    lost_sql = '''
    SELECT user_id,
           vip,
           to_date(act_time) as act_time
    FROM mid_info_all
    WHERE ds = '{date}'
    '''.format(date=date)
    lost_df = hql_to_df(lost_sql)
    lost_df['act_time'] = pd.to_datetime(lost_df['act_time'])
    lost_df = lost_df.groupby(['vip', 'act_time']).count().reset_index()
    lost_df['ds'] = pd.Timestamp(date)
    lost_df['loss_day'] = (
        lost_df['ds'] - lost_df['act_time']) / np.timedelta64(1, 'D')
    lost_df = lost_df.groupby(['vip', 'loss_day']).sum().user_id.reset_index()
    lost_3 = lost_df[['vip', 'user_id']][(lost_df['loss_day'] >= 2) & (lost_df['loss_day'] <= 3)].groupby(
        'vip').sum().user_id.reset_index().rename(columns={'user_id': 'lost_3'})
    lost_4_7 = lost_df[['vip', 'user_id']][(lost_df['loss_day'] >= 4) & (lost_df['loss_day'] <= 7)].groupby(
        'vip').sum().user_id.reset_index().rename(columns={'user_id': 'lost_4_7'})
    lost_8_15 = lost_df[['vip', 'user_id']][(lost_df['loss_day'] >= 8) & (lost_df['loss_day'] <= 15)].groupby(
        'vip').sum().user_id.reset_index().rename(columns={'user_id': 'lost_8_15'})
    lost_16_30 = lost_df[['vip', 'user_id']][(lost_df['loss_day'] >= 16) & (lost_df['loss_day'] <= 30)].groupby(
        'vip').sum().user_id.reset_index().rename(columns={'user_id': 'lost_16_30'})
    lost_31 = lost_df[['vip', 'user_id']][lost_df['loss_day'] >= 31].groupby(
        'vip').sum().user_id.reset_index().rename(columns={'user_id': 'lost_31'})
    lost_days_df = lost_3.merge(lost_4_7, on='vip', how='outer').merge(lost_8_15, on='vip', how='outer').merge(
        lost_16_30, on='vip', how='outer').merge(lost_31, on='vip', how='outer').fillna(0)

    # 更新MySQL表
    table = 'dis_common_loss_user'
    del_sql = 'delete from {0} where ds = "{1}"'.format(table, date)
    update_mysql(table, lost_days_df, del_sql)


if __name__ == '__main__':
    # settings_dev.set_env('sanguo_kr')
    # date = '20170114'
    # dis_common_loss_user(date)
    settings_dev.set_env('sanguo_kr')
    for date in date_range('20170118','20170121'):
        dis_common_loss_user(date)
