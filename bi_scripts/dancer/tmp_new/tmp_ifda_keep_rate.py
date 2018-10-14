#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Dong Junshuang
Description : 获取指定设备号的留存数据
Time        : 2017.07.03
illustration:
'''
import settings_dev
import pandas as pd
from utils import ds_add
from utils import hqls_to_dfs

keep_days = range(1, 15)

if __name__ == '__main__':
    settings_dev.set_env('dancer_pub')
    ifda_df = pd.read_excel('/Users/kaiqigu/Documents/Excel/ifda_data.xlsx')
    info_sql = '''
    SELECT user_id,
           device_mark
    FROM mid_info_all
    WHERE ds ='20170705'
    '''
    act_sql = '''
    SELECT ds,
           user_id
    FROM parse_info
    WHERE ds >= '20170628'
      AND ds <= '20170705'
    '''
    info_df, act_df = hqls_to_dfs([info_sql, act_sql])
    reg_df = info_df.merge(ifda_df, on='device_mark')
    reg_df = reg_df.rename(columns={'ds': 'reg_ds'})
    reg_df['reg_user_num'] = 1
    reg_df['reg_ds'] = reg_df['reg_ds'].astype('string')

    # 活跃用户表转职后合并到注册表后，将活跃日期变成列名
    act_df['act'] = 1
    act_df = act_df.rename(columns={'ds': 'act_ds'})
    reg_act_df = (act_df.pivot_table('act', ['user_id'], 'act_ds')
                  .reset_index().merge(reg_df, how='right').reset_index())

    # # 求每一个受影响的日期留存率，然后合并
    # keep_rate_dfs = []
    # for reg_date in reg_df.drop_duplicates('reg_ds').reg_ds.tolist():
    #     reg_date = str(reg_date)
    #     act_dates = [ds_add(reg_date, keep_day - 1) for keep_day in keep_days]
    #     act_dates_dic = {ds_add(reg_date, keep_day - 1): 'd%d_keep' % keep_day
    #                      for keep_day in keep_days}
    #     keep_df = (reg_act_df.loc[reg_act_df.reg_ds == reg_date,
    #                               ['reg_user_num'] + act_dates_dic.keys()]
    #                .rename(columns=act_dates_dic).sum().fillna(0).to_frame().T)
    #     for c in act_dates_dic.values():
    #         keep_df[c + 'rate'] = keep_df[c] / keep_df.reg_user_num
    #     keep_df['ds'] = reg_date
    #     keep_df['dau'] = len(set(act_df.loc[
    #         act_df.act_ds == reg_date].user_id.tolist()) | set(reg_df.loc[
    #             reg_df.reg_ds == str(reg_date)].user_id.tolist()))
    #     keep_rate_dfs.append(keep_df)

    # keep_rate_df = pd.concat(keep_rate_dfs)
    # columns = ['ds', 'dau', 'reg_user_num'] + \
    #     ['d%d_keeprate' % d for d in keep_days]
    # result_df = keep_rate_df[columns]
    # print result_df

    # 求每一个受影响的日期留存率，然后合并
    keep_rate_dfs = []
    for reg_date in reg_df.drop_duplicates('reg_ds').reg_ds.tolist():
        reg_date = str(reg_date)
        act_dates = [ds_add(reg_date, keep_day - 1) for keep_day in keep_days]
        act_dates_dic = {ds_add(reg_date, keep_day - 1): 'd%d_keep' % keep_day
                         for keep_day in keep_days}
        keep_df = (reg_act_df.loc[reg_act_df.reg_ds == reg_date, [
            'reg_user_num', 'device_mark'] + act_dates_dic.keys()]
            .groupby('device_mark').max().reset_index()
            .rename(columns=act_dates_dic).sum().fillna(0).to_frame().T)
        for c in act_dates_dic.values():
            keep_df[c + 'rate'] = keep_df[c] / keep_df.reg_user_num
        keep_df['ds'] = reg_date
        # keep_df['dau'] = len(set(act_df.loc[
        #     act_df.act_ds == reg_date].user_id.tolist()) | set(reg_df.loc[
        #         reg_df.reg_ds == str(reg_date)].user_id.tolist()))
        keep_rate_dfs.append(keep_df)

    keep_rate_df = pd.concat(keep_rate_dfs)
    columns = ['ds', 'reg_user_num'] + \
        ['d%d_keeprate' % d for d in keep_days]
    result_df = keep_rate_df[columns]
    print result_df

    result_df.to_excel('/Users/kaiqigu/Documents/Excel/idfa_keep.xlsx')
