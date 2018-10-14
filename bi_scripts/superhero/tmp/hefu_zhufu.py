#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Dong Junshuang
Description : 重写合服数据脚本
date        : 2017.04.10
注：主服战斗力前20的玩家
'''
import settings_dev
from utils import ds_add, hql_to_df
import pandas as pd


# def get_ser_data():
#     '''
#     将[主、从服、从服..]数据：'vno4', 'vnq6', 'vnt0', 'vnu0'],
#     生成为[主服\t从服]数据
#     并返回[主服、从服]数据的DataFrame
#     '''
#     path = r'/Users/kaiqigu/Documents/Excel/'
#     file_in = path + 'vn_ser_list.txt'
#     file_out = path + 'vn_ser_result.txt'
#     f_in = open(file_in, 'r')
#     f_out = open(file_out, 'w')
#     f_out.write(str('father_ser') + '\t' + str('son_ser') + '\n')
#     try:
#         for l_raw in f_in:
#             try:
#                 for i in tuple(eval(l_raw))[0]:
#                     f_out.write(str(tuple(eval(l_raw))[0][0]) + '\t' + str(i) +
#                                 '\n')
#             except:
#                 print 'error !!!'
#     finally:
#         f_in.close()
#         f_out.close()
#     print 'ser date complete'
#     ser_df = pd.read_table(file_out, sep='\t')
#     return ser_df


if __name__ == '__main__':
    settings_dev.set_env('superhero_qiku')
    date = '20170416'
    plat = 'q'
    # 获取每个子服的数据
    info_sql = '''
    SELECT a.ds,
           reverse(substring(reverse(a.uid), 8)) AS son_ser,
           a.uid,
           a.zhandouli,
           nvl(b.order_rmb,0) order_rmb
    FROM
      (SELECT ds,
              uid,
              zhandouli
       FROM raw_info
       WHERE substr(uid,1,1) = '{plat}'
         AND ds >= '{date_ago}'
         AND ds <= '{date}')a
    LEFT OUTER JOIN
      (SELECT ds,
              uid,
              sum(order_money) order_rmb
              -- sum(order_rmb) order_rmb
       FROM raw_paylog
       WHERE substr(uid,1,1) = '{plat}'
         AND ds >= '{date_ago}'
         AND ds <= '{date}'
         AND platform_2 <> 'admin_test'
       GROUP BY ds,
                uid )b ON a.ds = b.ds
    AND a.uid = b.uid
    '''.format(date_ago=ds_add(date, -6),
               date=date, plat=plat)

    info_df = hql_to_df(info_sql)

    # 获取主服、从服数据
    path = r'/Users/kaiqigu/Documents/Excel/'
    file_out = path + 'qiku_ser_result.txt'
    ser_df = pd.read_table(file_out, sep='\t')

    info_ser = info_df.merge(ser_df, on='son_ser')

    # 前十平均战斗力
    battle_df = (info_ser
                 .groupby(['father_ser', 'uid'])
                 .max()
                 .zhandouli
                 .reset_index()
                 )
    dfs = []
    for father_ser in battle_df.drop_duplicates('father_ser')['father_ser'].tolist():
        ser_battle_df = battle_df[battle_df.father_ser == father_ser]
        ser_battle_df = ser_battle_df.sort_values(
            by='zhandouli', ascending=False)
        ser_battle_df['zhandouli'] = ser_battle_df['zhandouli'].astype('float')
        battle_data = ser_battle_df[0:20]
        # battle_data =  (ser_battle_df[0:10]
        #                 .groupby('father_ser')
        #                 .mean()
        #                 .reset_index()
        #                 .rename(columns={'zhandouli':'avg_battle'})
        #                 )
        dfs.append(battle_data)
    battle_result = pd.concat(dfs)

    # columns = ['father_ser','avg_dau','pay_num','pay_money','max_battle','avg_battle']
    # result_df = result_df[columns]

    battle_result.to_excel(
        '/Users/kaiqigu/Documents/Excel/zhufu_data_{plat}_{date}.xlsx'.format(plat=plat, date=date))

    # # 主服前20平均战斗力
    # battle_df = (info_df.groupby(['son_ser', 'uid']).max()
    #              .zhandouli.reset_index())
    # dfs = []
    # father_list = ser_df.drop_duplicates('father_ser')['father_ser'].tolist()
    # for father_ser in father_list:
    #     ser_battle_df = battle_df[battle_df.son_ser == father_ser]
    #     ser_battle_df = ser_battle_df.sort_values(by='zhandouli',
    #                                               ascending=False)
    #     ser_battle_df['zhandouli'] = ser_battle_df['zhandouli'].astype('float')
    #     battle_data = ser_battle_df[0:20]
    #     dfs.append(battle_data)
    # battle_result = pd.concat(dfs)

    # battle_result.to_excel(
    #     '/Users/kaiqigu/Documents/Excel/zhufu_data_{plat}_{date}.xlsx'.format(
    #         plat=plat, date=date))
