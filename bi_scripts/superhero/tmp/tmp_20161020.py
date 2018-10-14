#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Dong Junshuang
Description : UID钻石消费(钻石数据从行为日志中取)
'''
from utils import hqls_to_dfs, ds_add, update_mysql, date_range
import settings

def coin_spend(date,pp):
    sum_sql = '''
    SELECT uid,
           stmp,
           pre_coin,
           post_coin,
           row_number() over(partition BY uid
                             ORDER BY stmp ASC) r1,
                                                row_number() over(partition BY uid
                                                                  ORDER BY stmp DESC) r2
    FROM raw_action_log
    WHERE ds = '{date}'
    and substr(uid,1,1) = '{pp}'
    '''.format(date=date,pp=pp)
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
    sum_df, spend_df, pay_df = hqls_to_dfs([sum_sql, spend_sql, pay_sql])

    yes_df = sum_df[sum_df.r1 == 1][['uid','pre_coin']]
    day_df = sum_df[sum_df.r2 == 1][['uid','post_coin']]

    result_df = (yes_df.merge(day_df,on='uid')
        .merge(spend_df,on='uid',how = 'left')
        .merge(pay_df,on='uid',how = 'left')
        .fillna(0)
        )

    result_df['add_coin'] = result_df['post_coin'] - result_df['pre_coin']
    result_df['free_get_coin'] = result_df['spend_coin'] - result_df['pay_coin'] + result_df['add_coin']
    result_df['ds'] = date
    result_df = result_df.sort_index(by='free_get_coin',ascending=False)
    result_final_df = result_df.head(20)
    result_final_df['rank'] = range(1,21)

    result_final_df.to_excel('/Users/kaiqigu/Downloads/Excel/sup_pub1_{0}.xlsx'.format(date))


if __name__ == '__main__':
    settings.set_env('superhero_bi')
    pp = 'g'
    for date in date_range('20161015','20161018'):
        coin_spend(date,pp)
