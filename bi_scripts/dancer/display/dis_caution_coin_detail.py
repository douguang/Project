#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Hu Chunlong
Description : 武娘 预警系列
create_date : 2017.01.05
'''
import settings_dev
from utils import hql_to_df, update_mysql, date_range


def dis_caution_coin_detail(date):
    sql = '''
    SELECT user_id,
           a_typ,
           sum(freemoney_diff) AS free_coin
    FROM parse_actionlog
    WHERE ds = '{date}'
      AND freemoney_diff > 0
    GROUP BY user_id,
             a_typ
    ORDER BY sum(freemoney_diff) DESC
    '''.format(**{
        'date': date
    })
    df = hql_to_df(sql)
    # 将用户当日获得钻石汇总，并筛选出获得钻石>=10000的用户
    df_total = df.groupby('user_id').sum().rename(columns={'free_coin': 'all_free_coin'
                                                           }).reset_index()

    df_total = df_total[df_total['all_free_coin'] >= 10000].sort_values(
        ['all_free_coin'], ascending=False)
    # 将用户当日获得钻石根据===接口===汇总，并筛选出获得钻石>=10000的用户
    df_detail = df.groupby(['user_id', 'a_typ']).sum().reset_index()
    df_detail = df_detail.merge(
        df_total, on='user_id', how='inner')
    df_detail['ds'] = date
    columns = ['ds', 'user_id', 'a_typ', 'free_coin', 'all_free_coin']
    result_df = df_detail[columns]
    # 更新数据库
    table = 'dis_caution_coin_detail'
    print date, table
    del_sql = 'delete from {0} where ds="{1}"'.format(table, date)
    update_mysql(table, result_df, del_sql)


if __name__ == '__main__':
    for db in ['dancer_tw', 'dancer_pub']:
        for date in date_range('20170119', '20170121'):
            settings_dev.set_env(db)
            dis_caution_coin_detail(date)
