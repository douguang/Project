#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Hu Chunlong
Description : 僵尸用户

uid数据
默认显示100个僵尸用户名单，按照未充值天数进行排列，当有用户流失天数超过15天或进行过充值，则从名单中排除
僵尸用户定义：充值100元以上的7日活跃玩家，未进行充值3天以上的。
vip等级筛选需可进行多选
备注可手动添加
'''
import settings_dev
import pandas as pd
from utils import hql_to_df, update_mysql, ds_add, date_range
import datetime
import numpy as np


def dis_zombie_user(date):
    # mid_info_all找到用户的vip等级、钻石存量、战力值
    info_sql = '''
    SELECT t1.user_id AS user_id,
           vip,
           coin,
           combat,
           act_time,
           pay_all,
           last_pay_ds
    FROM
      (SELECT user_id,
              vip,
              coin,
              combat,
              act_time
       FROM mid_info_all
       WHERE ds = '{date}'
         AND user_id IN
           (SELECT DISTINCT user_id
            FROM raw_activeuser
            WHERE ds >= '{date_7}'
              AND ds <= '{date}')) t1
    JOIN
      (SELECT user_id,
              sum(order_money) AS pay_all,
              max(ds) AS last_pay_ds
       FROM raw_paylog
       WHERE ds <= '{date}'
         AND platform_2 != 'admin_test'
       GROUP BY user_id HAVING pay_all >= 100) t2 ON t1.user_id = t2.user_id
    '''.format(**{'date': date,
                  'date_7': ds_add(date, -6)})
    info_df = hql_to_df(info_sql)
    if info_df.__len__() != 0:
        today = pd.Period(date)
        info_df['not_pay_days'] = info_df.last_pay_ds.map(
            lambda ds: today - pd.Period(ds))
        # 僵尸用户分布
        ranges = [2, 3, 7, 15, 30, 5000]
        columns_to_rename = {
            '(15, 30]': 'not_pay_16_30',
            '(2, 3]': 'not_pay_3',
            '(3, 7]': 'not_pay_4_7',
            '(30, 5000]': 'not_pay_31',
            '(7, 15]': 'not_pay_8_15',
        }
        columns_to_show = ['vip', 'not_pay_3', 'not_pay_4_7', 'not_pay_8_15',
                           'not_pay_16_30', 'not_pay_31']
        info_df['ranges'] = pd.cut(info_df.not_pay_days, ranges)
        info_df['ranges'] = info_df['ranges'].astype('object')
        info_df['num'] = 1
        zombie_distr_df = pd.pivot_table(
            info_df, index=['vip'], columns=['ranges'], values='num', aggfunc=np.sum, fill_value=0).reset_index().rename(columns=columns_to_rename)
        for i in columns_to_show:
            if i not in zombie_distr_df.columns:
                zombie_distr_df[i] = 0
        zombie_distr_df['ds'] = date
        # 更新MySQL
        table = 'dis_zombie_distr'
        del_sql = 'delete from {0} where ds="{1}"'.format(table, date)
        update_mysql(table, zombie_distr_df[columns_to_show], del_sql)

        # 僵尸大R名单
        filter_act_time = str(datetime.datetime.strptime(
            date, '%Y%m%d') + datetime.timedelta(-15))
        zombie_user_df = info_df.loc[
            (info_df.act_time >= filter_act_time) & (info_df.not_pay_days > 3)].copy()
        zombie_user_df['ds'] = date
        zombie_user_columns_show = ['ds', 'user_id',
                                    'vip', 'coin', 'not_pay_days', 'combat']
        zombie_user_final_df = zombie_user_df[zombie_user_columns_show]

        # 更新MySQL
        table = 'dis_zombie_user'
        del_sql = 'delete from {0} where ds="{1}"'.format(table, date)
        update_mysql(table, zombie_user_final_df, del_sql)

if __name__ == '__main__':
    settings_dev.set_env('sanguo_ios')
    dis_zombie_user('20170216')
    # for platform in ['sanguo_ios']:
    #     settings_dev.set_env(platform)
    #     for date in date_range('20170121', '20170215'):
    #         dis_zombie_user(date)
