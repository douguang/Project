#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Dong Junshuang
Description : 合服数据需求
注：
mid_ser_map字段顺序：
主服指合服之后的服务器，对应son_ser；
从服指合服之前的服务器，对应father_ser；
玩家合服之后uid保持不变，解析uid得到的服务器为从服father_ser。
'''
import settings
from utils import hqls_to_dfs, update_mysql, ds_add, hql_to_df
from pandas import Series,DataFrame
import pandas as pd

if __name__ == '__main__':
    settings.set_env('superhero_vt')
    date = '20161102'
    info_sql = '''
    SELECT ds,
           reverse(substring(reverse(uid), 8)) AS father_ser,
           uid,
           zhandouli
    FROM raw_info
    WHERE substr(uid,1,1) = 'v'
         and ds >= '{date_ago}'
      AND ds <= '{date}'
    '''.format(date_ago=ds_add(date,-6),date = date)
    ser_sql ='''
    SELECT trim(son_ser) son_ser,
           trim(father_ser) father_ser
    FROM mid_ser_map
    WHERE ds ='{0}'
    '''.format(date)
    pay_sql = '''
    SELECT ds,
           reverse(substring(reverse(uid), 8)) AS father_ser,
           uid,
           order_money order_rmb
    FROM raw_paylog
    WHERE substr(uid,1,1) = 'v'
         and ds >= '{date_ago}'
      AND ds <= '{date}'
      and platform_2 <> 'admin_test'
    '''.format(date_ago=ds_add(date,-6),date = date)
    battle_sql ='''
     WITH a AS
      (SELECT trim(son_ser) son_ser,
                            trim(father_ser) father_ser
       FROM mid_ser_map
       WHERE ds ='{date}') ,
          b AS
      (SELECT ds,
              reverse(substring(reverse(uid), 8)) AS father_ser,
              uid,
              zhandouli
       FROM raw_info
       WHERE substr(uid,1,1) = 'v'
         and ds >= '{date_ago}'
         AND ds <= '{date}') ,
          c AS
      (SELECT a.son_ser son_ser,
                        a.father_ser father_ser,
                                     b.zhandouli zhandouli,
                                                 row_number() over(partition BY a.son_ser
                                                                   ORDER BY b.zhandouli DESC) AS rn
       FROM a
       JOIN b ON a.father_ser = b.father_ser)
    SELECT *
    FROM c
    WHERE rn <=10
    '''.format(date_ago=ds_add(date,-6),date = date)
    battle_df = hql_to_df(battle_sql)
    pay_df = hql_to_df(pay_sql)
    info_df,ser_df = hqls_to_dfs([info_sql,ser_sql])
    info_df = info_df.merge(ser_df,on='father_ser')
    pay_df = pay_df.merge(ser_df,on='father_ser')
    # 近7天平均dau
    info_result_df = info_df.groupby('son_ser').count().uid.reset_index()
    info_result_df['avg_dau'] = info_result_df['uid']*1.0/7
    del info_result_df['uid']
    # 近7天充值总人数
    pay_num_result_df = (pay_df.drop_duplicates(['son_ser','uid'])
                    .groupby('son_ser')
                    .count()
                    .uid
                    .reset_index()
                    .rename(columns={'uid':'pay_num'}))
    # 近7天充值总金额
    pay_money_result_df = (pay_df
                    .groupby('son_ser')
                    .sum()
                    .order_rmb
                    .reset_index()
                    .rename(columns={'order_rmb':'pay_money'}))
    # 最大战斗力
    max_battle_df = (info_df
                    .groupby('son_ser')
                    .max()
                    .zhandouli
                    .reset_index()
                    .rename(columns={'zhandouli':'max_battle'}))
    # 前十平均战斗力
    avg_battle_df = (battle_df
                    .groupby('son_ser')
                    .sum()
                    .zhandouli
                    .reset_index()
                    .rename(columns={'zhandouli':'sum_battle'}))
    avg_battle_df['avg_battle'] = avg_battle_df['sum_battle'] *1.0/10
    del avg_battle_df['sum_battle']
    result_df = (info_result_df.merge(pay_num_result_df,on='son_ser')
        .merge(pay_money_result_df,on='son_ser')
        .merge(max_battle_df,on='son_ser')
        .merge(avg_battle_df,on='son_ser')
        )

    columns = ['son_ser','avg_dau','pay_num','pay_money','max_battle','avg_battle']
    result_df = result_df[columns]

    server_df = info_df.loc[:,['son_ser','father_ser']].drop_duplicates(['son_ser','father_ser'])
    server_df = server_df.sort_index(by=['son_ser','father_ser'],ascending=True)
    server_df = server_df.rename(columns = {'son_ser':'main_ser','father_ser':'secd_ser'})

    result_df.to_excel('/home/kaiqigu/Documents/hefu_data11111.xlsx')
    # server_df.to_excel('/Users/kaiqigu/Downloads/Excel/hefu_server_data.xlsx')


