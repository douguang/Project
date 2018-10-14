#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Dong Junshuang
Description : 钻石存量
create_date : 2016.08.03
'''
import settings_dev
import pandas as pd
from utils import ds_add, hql_to_df, update_mysql, get_config, hqls_to_dfs, date_range

def dis_coin_save(date):
    ranges = [0, 19, 29, 39, 49, 59, 69, 79, 89, 99, 100]
    columns_to_rename = {
        '(0, 19]': '0_19',
        '(19, 29]': '20_29',
        '(29, 39]': '30_39',
        '(39, 49]': '40_49',
        '(49, 59]': '50_59',
        '(59, 69]': '60_69',
        '(69, 79]': '70_79',
        '(79, 89]': '80_89',
        '(89, 99]': '90_99',
        '(99, 100]': '99_100',
    }

    coin_sql = '''
    SELECT uid,
           level,
           CASE
               WHEN (nvl(diamond_free,0)+nvl(diamond_charge,0)) >=50000 THEN '50000+'
               WHEN (nvl(diamond_free,0)+nvl(diamond_charge,0)) <= 3000 THEN '3000-'
               WHEN (nvl(diamond_free,0)+nvl(diamond_charge,0)) >=3001
                    AND (nvl(diamond_free,0)+nvl(diamond_charge,0)) <=5000 THEN '3000_5000'
               WHEN (nvl(diamond_free,0)+nvl(diamond_charge,0)) >=5001
                    AND (nvl(diamond_free,0)+nvl(diamond_charge,0)) <=10000 THEN '5000_10000'
               WHEN (nvl(diamond_free,0)+nvl(diamond_charge,0)) >=10001
                    AND (nvl(diamond_free,0)+nvl(diamond_charge,0)) <=20000 THEN '10001_20000'
               WHEN (nvl(diamond_free,0)+nvl(diamond_charge,0)) >=20001
                    AND (nvl(diamond_free,0)+nvl(diamond_charge,0)) <=30000 THEN '20001_30000'
               WHEN (nvl(diamond_free,0)+nvl(diamond_charge,0)) >=30001
                    AND (nvl(diamond_free,0)+nvl(diamond_charge,0)) <=50000 THEN '30001_50000'
               ELSE 'None'
           END AS coin_num
    FROM parse_info
    WHERE ds = '{date}'
    '''.format(date=date)
    coin_df = hql_to_df(coin_sql)

    result_df = coin_df.groupby(['coin_num', pd.cut(coin_df.level, ranges)]).count().uid.fillna(0).reset_index()
    print result_df
    result_df = pd.pivot_table(result_df, values='uid', index=['coin_num'], columns='level', aggfunc='sum').fillna(0).rename(columns=columns_to_rename).reset_index()
    result_df['ds'] = date
    print result_df

    columns = ['ds','coin_num', '0_19','20_29','30_39','40_49','50_59','60_69','70_79','80_89','90_99','99_100']
    result_df = result_df[columns]
    print result_df

    # 更新MySQL
    table = 'dis_coin_save'
    del_sql = 'delete from {0} where ds="{1}"'.format(table, date)
    update_mysql(table, result_df, del_sql)

if __name__ == '__main__':
    settings_dev.set_env('crime_empire_pub')
    for date in date_range('20170512', '20170516'):
        dis_coin_save(date)
