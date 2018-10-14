#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Dong Junshuang
Description : 日常数据(留存率：uid)(批量补数据)
注：需手动删除更新的数据
'''
from utils import hqls_to_dfs, update_mysql,date_range,ds_add, hql_to_df
import settings_dev
import pandas as pd

# 要跑的留存日期
keep_days = [2, 3, 7, 15, 30]

if __name__ == '__main__':

    start_date = '20170501'
    end_date = '20170930'
    final_date = '20170930'
    # keep_days = [2,3,7]
    reg_sql = '''
    SELECT  ds,
            uid
       FROM raw_reg
       WHERE ds >= '{0}'
    and ds <= '{1}'
    '''.format(start_date,end_date)
    act_sql = '''
    SELECT ds,
           uid,
           platform_2
    FROM raw_info
    WHERE ds >= '{0}'
    '''.format(start_date)
    # reg_df, act_df = hqls_to_dfs([reg_sql,act_sql])
    settings_dev.set_env('superhero_bi')
    a_reg_df = hql_to_df(reg_sql)
    a_act_df = hql_to_df(act_sql)
    a_reg_df = a_reg_df.merge(a_act_df,on=['ds','uid'],how='left')

    settings_dev.set_env('superhero_qiku')
    q_reg_df = hql_to_df(reg_sql)
    q_act_df = hql_to_df(act_sql)
    q_reg_df = q_reg_df.merge(q_act_df, on=['ds', 'uid'], how='left')

    act_df = pd.concat([a_act_df, q_act_df])
    reg_df = pd.concat([a_reg_df, q_reg_df])

    date_list = date_range(start_date, end_date)
    reg_dates = [date for date in date_list]

    dfs = []
    for date in reg_dates:
        print date
        reg_data = reg_df.loc[reg_df.ds == date]
        reg_num = (
            reg_data.groupby('ds').count().reset_index()
                    .loc[:,['ds','uid']]
                    .rename(columns={'uid':'reg_user_num'}))
        result_df = reg_num
        keep_day_list = [ds_add(date,i-1) for i in keep_days]
        keep_day_dic = {ds_add(date,i-1):'d%d_keeprate' %i for i in keep_days}
        act_df['is_reg_ds'] = act_df['ds'].isin(keep_day_list)
        act_data = act_df[act_df['is_reg_ds']]
        act_data['act'] = 1
        for i in keep_days:
            keep_day = ds_add(date,i-1)
            # print i,keep_day
            act_data = act_df.loc[act_df.ds == keep_day]
            act_data['is_reg'] = act_data['uid'].isin(reg_data.uid.values)
            act_data = act_data[act_data['is_reg']]
            ltv_end_date = ds_add(date, i - 1)
            if ltv_end_date > final_date:
                keep_df = (
                act_data.groupby('ds').count().reset_index()
                    .loc[:,['ds', 'uid']]
                    .rename(columns={'uid':'d%d_keeprate' %i}))
                keep_df['d%d_keeprate' %i] = 0
            else:
                # print 'act_data'
                # print act_data.head(20)
                keep_df = (
                act_data.groupby('ds').count().reset_index()
                        .loc[:,['ds', 'uid']]
                        .rename(columns={'uid':'d%d_keeprate' %i}))
                keep_df['ds'] = date
            result_df = result_df.merge(keep_df,on='ds',how='left')
        result_df['ds'] = date
        dfs.append(result_df)
    df = pd.concat(dfs)
    result_df = df.fillna(0)
    # for i in ['d%d_keeprate' %i for i in keep_days]:
    #     result_df[i] = df[i]/df['reg_user_num']
    # print result_df
    columns = ['ds', 'reg_user_num'] + ['d%d_keeprate' % d
                                             for d in keep_days]
    result_df = result_df[columns]
    result_df = result_df.fillna(0)
    print result_df
    result_df.to_excel(r'E:\Data\output\superhero\keep_rate.xlsx')