#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Dong Junshuang
Description : 钻石消耗排名
Time        : 2017.05.02
illustration:
'''
import settings_dev
import pandas as pd
from utils import hqls_to_dfs
from utils import hql_to_df
from utils import update_mysql


# def main():
#     pass


# if __name__ == '__main__':
#     # main()
#     settings_dev.set_env('superhero_qiku')
#     path = r'/Users/kaiqigu/Documents/Excel/'
#     file_out = path + 'qiku_ser_result.txt'
#     ser_df = pd.read_table(file_out, sep='\t')
#     spendlog_sql = '''
#     select ds,uid,reverse(substring(reverse(uid), 8)) AS son_ser,
#     subtime,
#     coin_num,
#     case when substr(subtime,12,8) <='12:00:00' then 1 else 0 end sub_12,
#     case when substr(subtime,12,8) <='21:00:00' then 1 else 0 end sub_21,
#     case when substr(subtime,12,8) <='23:59:59' then 1 else 0 end sub_24
#     from raw_spendlog
#     where ds in ('20170429','20170501')
#     '''
#     mid_sql = '''
#     select distinct uid from mid_gs
#     '''
#     spendlog_df, mid_df = hqls_to_dfs([spendlog_sql, mid_sql])
#     result_df = spendlog_df[~spendlog_df['uid'].isin(
#         mid_df.uid.values)]
#     result_df = result_df.merge(ser_df, on='son_ser', how='left')
#     for times in ['sub_12', 'sub_21', 'sub_24']:
#         result = result_df[result_df[times] == 1].groupby(
#             ['ds', 'father_ser', 'uid']).sum().coin_num.reset_index()
#         dfs = []
#         for date in ['20170429', '20170501']:
#             for i in result.drop_duplicates('father_ser').father_ser.tolist():
#                 df = result[(result.ds.values == date) &
#                             (result.father_ser.values == i)]
#                 df = df.sort_values(by='coin_num', ascending=False)
#                 df['rank'] = range(1, (len(df) + 1))
#                 dfs.append(df)
#         result_data = pd.concat(dfs)
#         result_data.to_excel(
#             '/Users/kaiqigu/Documents/Excel/result_{0}.xlsx'.format(times))


if __name__ == '__main__':
    settings_dev.set_env('superhero_bi')
    sql = '''
    SELECT user_id,
        name,
        server,
        sum_money
    FROM
    (SELECT user_id,
            name,
            reverse(substring(reverse(user_id), 8)) AS server,
            sum(order_money) sum_money
    FROM mart_assist
    WHERE ds in('20170429','20170430')
    GROUP BY user_id,
                name,
                reverse(substring(reverse(user_id), 8)) ) a
    WHERE sum_money >= 2000
    '''
    df = hql_to_df(sql)

    df.to_csv('/Users/kaiqigu/Documents/report/df',
              sep='\t', index=False, header=False)
