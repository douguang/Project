#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Dong Junshuang、Zhang Yongchen
Description : pub 滚服数据(包括支付数据)
create_date : 2016.09.01
'''
import settings_dev
import pandas as pd
from utils import ds_add, hqls_to_dfs, hql_to_df

def roll_server(server,server_ago,date):
    settings_dev.set_env('superhero_bi')
    server_list = [server]
    server_ago_list = [server_ago]
    info_sql ='''
    SELECT ds,reverse(substr(reverse(uid), 8)) AS server,
           uid,
           device
    FROM
      (SELECT ds,uid,
              device
       FROM raw_info
       WHERE  ds >= '{date}')a LEFT semi
    JOIN
      (SELECT ds,uid
       FROM raw_reg
       WHERE ds >= '{date}') b ON a.uid = b.uid
       and a.ds = b.ds
    '''.format(date = date)
    info_ago_sql = '''
    SELECT ds,reverse(substr(reverse(uid), 8)) AS server,
           uid,
           device
    FROM mid_info_all
    WHERE ds = '{0}'
    '''.format(ds_add(date,-1))
    pay_sql = '''
    SELECT ds,
           reverse(substr(reverse(uid), 8)) AS server,
           uid,
           order_money
    FROM raw_paylog
    WHERE ds >= '{0}'
    '''.format(date)

    # info_df,pay_df,info_ago_df = hqls_to_dfs([info_sql,pay_sql,info_ago_sql])
    info_df = hql_to_df(info_sql)
    print info_df.head(10)
    pay_df = hql_to_df(pay_sql)
    print pay_df.head(10)
    info_ago_df = hql_to_df(info_ago_sql)
    print info_ago_df.head(10)

    dfs = []
    for num,server in enumerate(server_list):
        info_result_df = info_df.loc[info_df.server == server]
        pay_result_df = pay_df.loc[pay_df.server == server]
        # 新增玩家
        new_user_df = (info_result_df
                            .groupby(['ds','server'])
                            .count()
                            .uid
                            .reset_index()
                            .rename(columns={'uid':'new_user'}))
        # 充值总人数
        pay_user_df = (pay_result_df
                            .drop_duplicates(['ds','server','uid'])
                            .groupby(['ds','server'])
                            .count()
                            .uid
                            .reset_index()
                            .rename(columns={'uid':'pay_user'}))
        # 总收入
        pay_money_df = (pay_result_df
                            .groupby(['ds','server'])
                            .sum()
                            .order_money
                            .reset_index()
                            .rename(columns={'order_money':'income'}))

        server_df = info_df.loc[info_df.server == server]
        other_df = info_ago_df.loc[info_ago_df.server != server]
        server_df['is_roll'] = server_df['device'].isin(other_df.device.values)
        server_df = server_df[server_df['is_roll']]
        # 滚服玩家
        roll_user_df = (server_df
                            .groupby(['ds','server'])
                            .count()
                            .uid
                            .reset_index()
                            .rename(columns={'uid':'roll_user'}))
        # 滚服充值玩家
        pay_df['is_roll'] = pay_df['uid'].isin(server_df.uid.values)
        pay_roll_df = pay_df[pay_df['is_roll']]
        roll_pay_user_df = (pay_roll_df
                            .drop_duplicates(['ds', 'server', 'uid'])
                            .groupby(['ds','server'])
                            .count()
                            .uid
                            .reset_index()
                            .rename(columns={'uid':'roll_pay_user'}))
        # 滚服收入
        roll_pay_money_df = (pay_roll_df
                            .groupby(['ds','server'])
                            .sum()
                            .order_money
                            .reset_index()
                            .rename(columns={'order_money':'roll_money'}))
        # 来自前一个服的滚服玩家
        server_df = info_df.loc[info_df.server == server]
        other_df = info_ago_df.loc[info_ago_df.server == server_ago_list[num]]
        server_df['is_roll'] = server_df['device'].isin(other_df.device.values)
        server_df = server_df[server_df['is_roll']]
        roll_ago_user_df = (server_df
                            .groupby(['ds','server'])
                            .count()
                            .uid
                            .reset_index()
                            .rename(columns={'uid':'roll_ago_user'}))

        result_df = (new_user_df.merge(pay_user_df,on=['ds','server'],how='left')
                                .merge(pay_money_df,on=['ds','server'],how='left')
                                .merge(roll_user_df,on=['ds','server'],how='left')
                                .merge(roll_pay_user_df,on=['ds','server'],how='left')
                                .merge(roll_pay_money_df,on=['ds','server'],how='left')
                                .merge(roll_ago_user_df,on=['ds','server'],how='left'))
        dfs.append(result_df)

    result = pd.concat(dfs)

    columns =['ds','server','new_user','roll_user', 'roll_ago_user', 'pay_user', 'roll_pay_user', 'income', 'roll_money']
    result_pay = result[columns]
    print result_pay
    # return result_pay

    # 滚服前一个服的数据
    # columns =['ds','server','new_user','roll_user','roll_ago_user']
    # result_pay = result[columns]
    result_pay.to_excel(r'E:\Data\output\superhero\roll_%s.xlsx'%server)

if __name__ == '__main__':
    date = '20170519'
    server = 673
    server_ago = 672
    # result_list = []
    for i in range(0, 6, 1):
        date = ds_add(date, 2)
        server = server + 1
        server_str = 'g' + str(server)
        server_ago = server_ago + 1
        server_ago_str = 'g' + str(server_ago)
        roll_server(server_str, server_ago_str, date)
        # result_list.append(roll_server(server_str, server_ago_str, date))
    # result_df = pd.concat(result_list)
    # result_df.to_excel(r'E:\Data\output\superhero\result_df.xlsx')