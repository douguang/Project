#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Dong Junshuang
Description : 处理args数据 - 限时兑换
Time        : 2017.06.26
illustration:
限时兑换ID  限时兑换次数
'''
import settings_dev
import pandas as pd
from utils import hql_to_df

id_list = [
    8046, 8047, 8048, 8049, 8050, 8051, 8052, 8053, 8054, 8055, 8056, 8057,
    8058, 8059, 8060, 8061, 8062, 8063, 8064, 8065
]

if __name__ == '__main__':
    settings_dev.set_env('superhero_vt')
    sql = '''
    SELECT ds,
           uid,
           args
    FROM raw_action_log
    WHERE ds >='20170623' and ds <='20170625'
      AND action = 'omni_exchange.omni_exchange'
    '''
    df = hql_to_df(sql)

    def get_value():
        for _, row in df.iterrows():
            yield [row.ds, row.uid, eval(row.args)['id'][0]]

    column = ['ds', 'uid', 'id']
    result = pd.DataFrame(get_value(), columns=column)
    result['id'] = result['id'].astype('int')
    result_df = result[result['id'].isin(id_list)]
    result_df = result_df.groupby('id').agg({
        'ds': 'count',
        'uid': 'nunique',
    }).reset_index().rename(columns={'ds': 'times',
                                     'uid': 'num'})
    result_df.to_excel('/Users/kaiqigu/Documents/Excel/exchange.xlsx')
