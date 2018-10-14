#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Dong Junshuang
Description : pub 滚服数据
create_date : 2016.08.29
'''
import settings
import pandas as pd
from utils import ds_add, hql_to_df, update_mysql, get_config, hqls_to_dfs

if __name__ == '__main__':
    settings.set_env('superhero_bi')
    start_date = '20160520'
    end_date = '20160604'
    info_sql ='''
    SELECT ds,reverse(substr(reverse(uid), 8)) AS server,
           uid,
           account
    FROM
      (SELECT ds,uid,
              account
       FROM raw_info
       WHERE ds >= '{start_date}'
       and ds <= '{end_date}')a LEFT semi
    JOIN
      (SELECT ds,uid
       FROM raw_reg
       WHERE ds >= '{start_date}'
       and ds <= '{end_date}') b ON a.uid = b.uid
       and a.ds = b.ds
    '''.format(start_date = start_date,end_date=end_date)
    info_ago_sql = '''
    SELECT ds,reverse(substr(reverse(uid), 8)) AS server,
           uid,
           account
    FROM mid_info_all
    WHERE ds >= '{start_date_ago}'
       and ds <= '{end_date_ago}'
    '''.format(start_date_ago = ds_add(start_date,-1),end_date_ago=ds_add(end_date,-1))
    pay_sql = '''
    SELECT ds,
           reverse(substr(reverse(uid), 8)) AS server,
           uid,
           order_money
    FROM raw_paylog
    WHERE ds >= '{start_date}'
      AND ds <= '{end_date}'
    '''.format(start_date = start_date,end_date=end_date)

    info_df,pay_df,info_ago_df = hqls_to_dfs([info_sql,pay_sql,info_ago_sql])

    info_result_df = info_df.loc[(info_df.server >= 'g511') & (info_df.server <= 'g515')]
    pay_result_df = pay_df.loc[(pay_df.server >= 'g511') & (pay_df.server <= 'g515')]

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

    server_list = info_result_df.drop_duplicates('server')['server'].tolist()

    dfs,pay_num_dfs,pay_money_dfs = [],[],[]
    for server in server_list:
        if server == 'g511':
            date_list = ['20160520','20160521']
        if server == 'g512':
            date_list = ['20160523','20160524']
        if server == 'g513':
            date_list = ['20160527','20160528']
        if server == 'g514':
            date_list = ['20160530','20160531']
        if server == 'g515':
            date_list = ['20160603','20160604']
        for dt in date_list:
            dt_ago = ds_add(dt,-1)
            info_data = info_result_df.loc[info_result_df.ds == dt]
            info_ago_data = info_ago_df.loc[info_ago_df.ds == dt_ago]
            info_data['is_roll'] = info_data['account'].isin(info_ago_data.account.values)
            roll_df = info_data[info_data['is_roll']]
            # 滚服玩家
            roll_user_df = (roll_df
                                .groupby(['ds','server'])
                                .count()
                                .uid
                                .reset_index()
                                .rename(columns={'uid':'roll_user'}))
            dfs.append(roll_user_df)
            # 滚服充值玩家
            pay_data = pay_df.loc[pay_df.ds == dt]
            roll_pay_data = pay_data.merge(roll_df,on=['ds','server','uid'])
            # pay_data['is_pay'] = pay_data['uid'].isin(roll_df.uid.values)
            # roll_pay_data = pay_data[pay_data['is_pay']]
            roll_pay_user_df = (roll_pay_data
                                .groupby(['ds','server'])
                                .count()
                                .uid
                                .reset_index()
                                .rename(columns={'uid':'roll_pay_user'}))
            pay_num_dfs.append(roll_pay_user_df)
            # 滚服收入
            roll_pay_money_df = (roll_pay_data
                                .groupby(['ds','server'])
                                .sum()
                                .order_money
                                .reset_index()
                                .rename(columns={'order_money':'roll_money'}))
            pay_money_dfs.append(roll_pay_money_df)

    roll_result_df = pd.concat(dfs)
    roll_pay_num_df = pd.concat(pay_num_dfs)
    roll_pay_money_df = pd.concat(pay_money_dfs)


    result_df = (new_user_df.merge(pay_user_df,on=['ds','server'])
                            .merge(pay_money_df,on=['ds','server'])
                            .merge(roll_result_df,on=['ds','server'])
                            .merge(roll_pay_num_df,on=['ds','server'])
                            .merge(roll_pay_money_df,on=['ds','server']))

    result_df = result_df.sort_index(by='server')

    columns =['ds','server','new_user','roll_user','pay_user','roll_pay_user','income','roll_money']
    result_df = result_df[columns]

    result_df.to_excel('/Users/kaiqigu/Downloads/Excel/roll_server.xlsx')




