#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Dong Junshuang
Description : 古灵阁活动参与情况
Create date : 2016.08.25
'''
import settings
from utils import hqls_to_dfs, update_mysql, ds_add
from pandas import Series,DataFrame
from utils import get_config
import pandas as pd


if __name__ == '__main__':
    settings.set_env('superhero_vt')
    date = '20161002'
    spend_sql = '''
    SELECT ds,uid
    FROM raw_spendlog
    WHERE ds ='{date}'
      AND goods_type = 'gringotts.investing'
    '''.format(date=date)
    pay_sql = '''
    SELECT uid,
           order_rmb
    FROM raw_paylog
    WHERE ds='{date}'
    '''.format(date=date)
    info_sql = '''
    SELECT uid,
           vip_level
    FROM raw_info
    WHERE ds = '{date}'
    '''.format(date=date)
    spend_df,pay_df,info_df = hqls_to_dfs([spend_sql,pay_sql,info_sql])
    spend_df = spend_df.merge(info_df,on='uid')
    pay_df = pay_df.merge(info_df,on='uid')
    # dau
    dau_df = (info_df.groupby('vip_level')
                .count().uid
                .reset_index()
                .rename(columns={'uid':'dau'}))
    # 参与人数
    attend_user_df = (spend_df.drop_duplicates(['ds','uid','vip_level'])
                        .groupby('vip_level')
                        .count().uid
                        .reset_index()
                        .rename(columns={'uid':'attend_user_num'}))
    # 参与次数
    attend_num_df = (spend_df.groupby('vip_level')
                        .count().uid
                        .reset_index()
                        .rename(columns={'uid':'attend_num'}))
    # 充值
    pay_df['is_attend'] = pay_df['uid'].isin(spend_df.uid.values)
    pay_result_df = pay_df[pay_df['is_attend']]
    pay_money_df = (pay_result_df.groupby('vip_level')
        .sum().order_rmb
        .reset_index()
        )
    # 每个uid投资的次数
    touzi_num_df = (spend_df.groupby('uid')
            .count().ds.reset_index()
            .rename(columns={'ds':'zouzi_num'}))
    # 投资9次的充值金额
    touzi_uid_df = touzi_num_df.loc[touzi_num_df.zouzi_num == 9]
    pay_df['is_touzi9'] = pay_df['uid'].isin(touzi_uid_df.uid.values)
    touzi_result_df = pay_df[pay_df['is_touzi9']]
    touzi_9_money_df = (touzi_result_df.groupby('uid')
                        .sum().order_rmb
                        .reset_index())

    result_df = (dau_df.merge(attend_user_df,on='vip_level',how='left')
        .merge(attend_num_df,on='vip_level',how='left')
        .merge(pay_money_df,on='vip_level',how='left')
        .fillna(0)
        )

    v02_df = (result_df.loc[(result_df.vip_level >= 0)& (result_df.vip_level <= 2)]
        .sum()
        .to_frame()
        .T
        )
    v02_df['vip_level'] = '0-2'
    v35_df = result_df.loc[(result_df.vip_level >= 3)& (result_df.vip_level <= 5)]
    v6_df = (result_df.loc[result_df.vip_level >= 6]
         .sum()
        .to_frame()
        .T
        )
    v6_df['vip_level'] = '6+'

    result_df = pd.concat([v02_df,v35_df,v6_df])


    columns = ['vip_level','dau','attend_user_num','attend_num','order_rmb']
    result_df = result_df[columns]


    result_df.to_excel('/Users/kaiqigu/Downloads/Excel/guling_data.xlsx')
    touzi_9_money_df.to_excel('/Users/kaiqigu/Downloads/Excel/touzi9.xlsx')






