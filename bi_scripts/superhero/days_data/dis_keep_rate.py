#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Dong Junshuang
Description : 用户留存率(批量补数据)
注：需手动删除更新的数据
'''
from utils import hql_to_df, ds_add, update_mysql, format_dates, date_range
import settings
import pandas as pd

# 要跑的留存日期
keep_days = [2, 3, 4, 5, 6, 7, 14, 30, 60, 90]

if __name__ == '__main__':
    settings.set_env('superhero_vt')
    plat = 'superhero_vt'
    start_date = '20160501'
    end_date = '20160828'
    final_date = '20160828'

    reg_sql = '''
    SELECT reg_ds,
           uid
    FROM
      (SELECT ds AS reg_ds,
              uid
       FROM raw_reg
       WHERE ds >='{0}'
         AND ds<='{1}' )a LEFT semi
    JOIN
      ( SELECT ds ,
               uid
       FROM raw_info
       WHERE ds >='{0}'
         AND ds<='{1}' )b ON a.reg_ds = b.ds
    AND a.uid = b.uid
    '''.format(start_date,end_date)
    reg_df = hql_to_df(reg_sql)
    reg_df['reg_user_num'] = 1

    act_sql = '''
    SELECT ds AS act_ds,
           uid
    FROM raw_info
    WHERE ds >= '{0}'
    '''.format(start_date)
    act_df = hql_to_df(act_sql)

    dau_df = act_df.groupby('act_ds').count().reset_index()

    act_df['act'] = 1
    reg_act_df = (act_df
                  .pivot_table('act', ['uid'], 'act_ds')
                  .reset_index()
                  .merge(reg_df, how='right')
                  .reset_index()
                  )

    date_list = date_range(start_date,end_date)
    reg_dates = [date for date in date_list]

    dfs = []
    for date in reg_dates:
        act_dates = [ds_add(date, keep_day - 1) for keep_day in keep_days] + [date]
        act_dates_dic = {ds_add(date, keep_day - 1): 'd%d_keep' % keep_day for keep_day in keep_days}

        reg_date = date
        keep_df = (reg_act_df.loc[reg_act_df.reg_ds == reg_date,
                             ['reg_user_num'] + act_dates_dic.keys()]
                             .rename(columns=act_dates_dic)
                             .sum()
                             .fillna(0)
                             .to_frame()
                             .T)
        for c in act_dates_dic.values():
            keep_df[c + 'rate'] = keep_df[c] / keep_df.reg_user_num
        keep_df['ds'] = reg_date
        keep_df['dau'] = dau_df.loc[dau_df.act_ds == reg_date, 'uid'].values[0]
        columns = ['ds', 'dau', 'reg_user_num'] + ['d%d_keeprate' % d for d in keep_days]
        daily_keep_rate_df = keep_df[columns]
        dfs.append(daily_keep_rate_df)
    df = pd.concat(dfs)
    print df

    # 更新MySQL表
    table = 'dis_keep_rate'
    del_sql = 'delete from {0} where ds in {1}'.format(table, tuple(reg_dates))
    update_mysql(table, df, del_sql, plat)


