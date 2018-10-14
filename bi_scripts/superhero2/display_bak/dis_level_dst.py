#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Dong Junshuang
Description : 流失用户等级分布
'''
import settings_dev
from utils import hql_to_df, update_mysql, ds_add
import pandas as pd

def dis_level_dst(excute_date):
    '''流失的定义为一周未登陆，因此若传入的参数为 20160518，实际计算的是 20160511 的流失数据
    '''
    date = ds_add(excute_date, -7)
    print date
    level_dic = {i:'level_%d' % i for i in range(0,31)}
    level_list = ['level_%d' % i for i in range(0,31)]

    ranges = [30,40,50,60,70,80,90,100]
    columns_name = ['30_40','40_50','50_60','60_70','70_80','80_90','90_100']
    columns_to_rename = {
        '(30, 40]': '30_40',
        '(40, 50]': '40_50',
        '(50, 60]': '50_60',
        '(60, 70]': '60_70',
        '(70, 80]': '70_80',
        '(80, 90]': '80_90',
        '(90, 100]': '90_100',
    }

    info_sql = '''
    SELECT ds,
           user_id,
           level
    FROM parse_info
    WHERE ds >= '{date}'
      AND ds <= '{date_in_7days}'
    '''.format(**{
            'date': date,
            'date_in_7days': ds_add(date, 7),
        })
    info_df = hql_to_df(info_sql)

    # 全部用户
    total_info_df = info_df.loc[info_df.ds == date]
    total_info_num = total_info_df.count().user_id
    total_less_30_df = total_info_df.loc[total_info_df.level <= 30]
    total_more_30_df = total_info_df.loc[total_info_df.level > 30]
    # 30级以内
    total_less_30_df['total_num'] = 1
    total_less_30_df = (total_less_30_df
        .pivot_table('total_num', ['ds'], 'level')
        .reset_index()
        .rename(columns=level_dic)
        )
    for i in level_list:
        if i not in total_less_30_df.columns:
            total_less_30_df[i] = 0
    # 30级以上
    total_more_30_df = (total_more_30_df.groupby(['ds', pd.cut(total_more_30_df.level, ranges)])
             .count().user_id.reset_index()
             .pivot_table('user_id', ['ds'], 'level').reset_index()
             .fillna(0).rename(columns=columns_to_rename))
    # 总用户
    total_info_df = total_less_30_df.merge(total_more_30_df,on='ds',how ='outer').fillna(0)
    total_info_df['user_num'] = total_info_num

    columns = ['ds','user_num'] + level_list + columns_name
    total_info_df = total_info_df[columns]

    # 更新全部用户MySQL表
    table = 'dis_total_level_dst'
    print 'dis_total_level_dst'
    del_sql = 'delete from {0} where ds="{1}"'.format(table, date)
    update_mysql(table, total_info_df, del_sql)

    # 流失用户
    today_info_df = info_df.loc[info_df.ds == date]
    day7_info_df = info_df.loc[info_df.ds != date]
    today_info_df['is_loss'] = today_info_df['user_id'].isin(day7_info_df.user_id.values)
    loss_df = today_info_df[~today_info_df['is_loss']]
    loss_less_30_df = loss_df.loc[loss_df.level <= 30]
    loss_more_30_df = loss_df.loc[loss_df.level > 30]
    loss_df_num = loss_df.count().user_id
    loss_less_30_df['total_num'] = 1
    # 30级以内
    loss_less_30_df = (loss_less_30_df
        .pivot_table('total_num', ['ds'], 'level')
        .reset_index()
        .rename(columns=level_dic)
        )
    for i in level_list:
        if i not in loss_less_30_df.columns:
            loss_less_30_df[i] = 0
    # 30级以上
    loss_more_30_df = (loss_more_30_df.groupby(['ds', pd.cut(loss_more_30_df.level, ranges)])
             .count().user_id.reset_index()
             .pivot_table('user_id', ['ds'], 'level').reset_index()
             .fillna(0).rename(columns=columns_to_rename))
    # 总用户
    loss_df = loss_less_30_df.merge(loss_more_30_df,on='ds',how ='outer').fillna(0)
    loss_df['user_num'] = loss_df_num

    columns = ['ds','user_num'] + level_list + columns_name
    loss_df = loss_df[columns]

    # 更新流失用户MySQL表
    table = 'dis_loss_level_dst'
    print 'dis_loss_level_dst'
    del_sql = 'delete from {0} where ds="{1}"'.format(table, date)
    update_mysql(table, loss_df, del_sql)


if __name__ == '__main__':
    settings_dev.set_env('superhero2')
    date = '20170516'
    dis_level_dst(date)
