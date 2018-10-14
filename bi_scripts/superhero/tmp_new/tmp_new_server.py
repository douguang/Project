#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Dong Junshuang
Description : 新服数据(分服)
Time        : 2017.06.05
illustration: 日期、新增、DAU、VIP人数、总收入、付费人数、新服DAU、新服VIP人数、新服总收入、新服付费人数
注：已排除测试用户
'''
import settings_dev
import pandas as pd
from utils import hql_to_df

server_dic = {
    # 'vnes': '20170604',
    # 'vner': '20170602',
    # 'vneq': '20170530',
    # 'vnep': '20170527',
    # 'vneo': '20170524',
    # 'vnen': '20170521'
    'g681': '20170604',
    'g680': '20170602',
    'g679': '20170531',
    'g678': '20170529',
    'g677': '20170527',
    'g676': '20170525',
    'g675': '20170523'
}

if __name__ == '__main__':
    settings_dev.set_env('superhero_bi')
    start_date = '20170529'
    end_date = '20170604'
    info_sql = '''
    SELECT ds,
           user_id,
           server,
           vip,
           order_money,
           is_new_user
    FROM mart_assist
    WHERE ds >='{start_date}'
      AND ds <='{end_date}'
      -- pub的新服数据
      and plat = 'g'
      AND user_id NOT IN
        (SELECT uid
         FROM mid_gs)
    '''.format(start_date=start_date, end_date=end_date)
    info_df = hql_to_df(info_sql)
    info_df['is_vip'] = info_df['vip'].map(lambda s: 1 if s > 0 else 0)
    info_df['is_pay'] = info_df['order_money'].map(lambda s: 1 if s > 0 else 0)
    dau_df = info_df.groupby('ds').agg({
        'is_new_user': 'sum',
        'user_id': 'count',
        'is_vip': 'sum',
        'order_money': 'sum',
        'is_pay': 'sum',
    }).reset_index().rename(columns={'is_new_user': 'new_user',
                                     'user_id': 'dau',
                                     'is_vip': 'vip_num',
                                     'order_money': 'sum_money',
                                     'is_pay': 'pay_num'})
    # 开服时间
    server_df = pd.DataFrame(server_dic.items(),
                             columns=['server', 'start_date'])
    info_result = info_df.merge(server_df, on='server')

    # 获得开服天数
    def get_data():
        for _, row in info_result.iterrows():
            ds = pd.Period(row.ds)
            start_date = pd.Period(row.start_date)
            days = ds - start_date
            yield [row.ds, row.user_id, row.server, row.vip, row.order_money,
                   row.is_new_user, row.is_vip, row.is_pay, days]
    # 生成DataFrame
    result_df = pd.DataFrame(get_data(),
                             columns=['ds', 'user_id', 'server', 'vip',
                                      'order_money', 'is_new_user', 'is_vip',
                                      'is_pay', 'days'])
    # 新服数据
    new_server_df = result_df.loc[result_df.days <= 7]
    new_server_result = new_server_df.groupby('ds').agg({
        'is_new_user': 'sum',
        'user_id': 'count',
        'is_vip': 'sum',
        'order_money': 'sum',
        'is_pay': 'sum',
    }).reset_index().rename(columns={'is_new_user': 'server_new_user',
                                     'user_id': 'server_dau',
                                     'is_vip': 'server_vip_num',
                                     'order_money': 'server_sum_money',
                                     'is_pay': 'server_pay_num'})
    result = dau_df.merge(new_server_result, on='ds')
    column = ['ds', 'new_user', 'dau', 'vip_num', 'sum_money', 'pay_num',
              'server_dau', 'server_vip_num', 'server_sum_money',
              'server_pay_num']
    result = result[column]
    result.to_excel('/Users/kaiqigu/Documents/Excel/new_server_data.xlsx')

