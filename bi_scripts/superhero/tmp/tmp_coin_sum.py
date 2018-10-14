#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Dong Junshuang
Description : UID钻石消费
'''
from utils import hqls_to_dfs, ds_add, update_mysql, date_range
import settings

def coin_spend(date,pp):
    sum_sql = '''
    SELECT ds,
           uid,
           sum(zuanshi) sum_coin
    FROM mid_info_all
    WHERE ds IN ('{date_ago}',
                 '{date}')
    and substr(uid,1,1) = '{pp}'
    GROUP BY ds,
             uid
    '''.format(date=date, date_ago=ds_add(date, -1),pp=pp)
    spend_sql = '''
    SELECT uid,
           sum(coin_num) spend_coin
    FROM raw_spendlog
    WHERE ds='{date}'
    and substr(uid,1,1) = '{pp}'
    GROUP BY uid
    '''.format(date=date,pp=pp)
    pay_sql = '''
    SELECT uid,
           sum(order_coin) pay_coin
    FROM raw_paylog
    WHERE ds='{date}'
    and platform_2 <> 'admin_test'
    and substr(uid,1,1) = '{pp}'
    GROUP BY uid
    '''.format(date=date,pp=pp)
    info_sql = '''
    SELECT uid
    FROM raw_info
    WHERE ds='{date}'
      AND substr(uid,1,1) = '{pp}'
    '''.format(date=date,pp=pp)
    sum_df, spend_df, pay_df, info_df = hqls_to_dfs([sum_sql, spend_sql, pay_sql, info_sql])

    sum_dic = {ds_add(date, -1): 'yes_coin', date: 'day_coin'}
    sum_result_df = (sum_df.pivot_table(
        'sum_coin', 'uid', 'ds').reset_index().rename(columns=sum_dic))

    result_df = (info_df.merge(spend_df, on='uid', how='left')
                 .merge(sum_result_df, on='uid', how='left')
                 .merge(pay_df, on='uid', how='left').fillna(0))

    result_df['add_coin'] = result_df['day_coin'] - result_df['yes_coin']
    result_df['free_get_coin'] = result_df['spend_coin'] - result_df['pay_coin'] + result_df['add_coin']
    result_df['ds'] = date
    result_df = result_df.sort_index(by='free_get_coin',ascending=False)
    result_final_df = result_df.head(20)

    result_final_df.to_excel('/Users/kaiqigu/Downloads/Excel/ios_{0}.xlsx'.format(date))


if __name__ == '__main__':
    settings.set_env('superhero_bi')
    pp = 'a'
    for date in date_range('20161015','20161018'):
        coin_spend(date,pp)


