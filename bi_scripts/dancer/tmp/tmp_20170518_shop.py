#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Dong Junshuang
Description : 珍宝阁
Time        : 2017.05.17
'''
import settings_dev
from utils import hql_to_df

if __name__ == '__main__':
    settings_dev.set_env('dancer_tw')
    sql = '''
    SELECT user_id,
           a_tar
    FROM parse_actionlog
    WHERE ds = '20170515'
      AND a_typ = 'shop.black_shop_buy'
    '''
    df = hql_to_df(sql)
    df['shop_id'] = df['a_tar'].map(lambda s: eval(str(s))['shop_id'])
    df['shop_id'] = df['shop_id'].astype('int')
    id_list = [103, 104, 105]
    result_df = df[df['shop_id'].isin(id_list)]
    result_df = result_df.groupby('shop_id').user_id.count().reset_index()
    result_df.to_excel('/Users/kaiqigu/Downloads/zhenbaoge.xlsx')
