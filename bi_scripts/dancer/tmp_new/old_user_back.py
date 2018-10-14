#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Dong Junshuang
Description : 老玩家回归/掌门回归
Time        : 2017.06.07
illustration: 指定时间段内的玩家未活跃为老玩家
注：已排除测试用户，如最后留存日不是当前数据的最新日期，需往后跑3天数据才能查询到3留的数据
日期、回归的用户数、次日留存的用户数、次日留存率、3日留存的用户数、3日留存率、回归玩家的充值用户数、回归玩家的充值总额
'''
import settings_dev
import pandas as pd
from utils import ds_add
from utils import hql_to_df
from utils import date_range
from dancer.cfg import zichong_uids

keep_list = [2, 3]
zichong_uids = str(tuple(zichong_uids))

if __name__ == '__main__':
    settings_dev.set_env('dancer_tw')
    last_start_date = '20170601'
    last_end_date = '20170614'
    start_date = '20170615'
    end_date = '20170702'
    # last时间段活跃的用户
    act_sql = '''
    SELECT a.ds,
           a.user_id,
           nvl(b.sum_money,0) total_money,
           CASE
               WHEN sum_money >0 THEN 1
               ELSE 0
           END is_pay
    FROM
      (SELECT ds,
              user_id
       FROM parse_info
       WHERE ds >='{start_date}'
         AND ds <='{end_date}'
         and level >= 35
         AND regexp_replace(substr(reg_time,1,10),'-','') < '{start_date}'
         AND user_id NOT IN
           (SELECT DISTINCT user_id
            FROM parse_info
            WHERE ds >= '{last_start_date}'
              AND ds <='{last_end_date}')) a
    LEFT OUTER JOIN
      (SELECT ds,
              user_id,
              sum(order_money) sum_money
       FROM raw_paylog
       WHERE ds >='{start_date}'
         AND ds <='{end_date}'
         AND platform_2 <> 'admin_test'
         AND order_id NOT LIKE '%testktwwn%'
       GROUP BY ds,
                user_id) b ON a.ds = b.ds
    AND a.user_id = b.user_id
    where a.user_id not in {zichong_uids}
    '''.format(start_date=start_date,
               end_date=end_date,
               last_start_date=last_start_date,
               last_end_date=last_end_date,
               zichong_uids=zichong_uids)
    act_df = hql_to_df(act_sql)
    pay_df = act_df.groupby('ds').sum().reset_index()

    dfs = []
    for date in date_range(start_date, end_date):
        act_dates = [ds_add(date, i - 1) for i in keep_list]
        act_dates_dic = {ds_add(date, i - 1): 'd%d_keep' % i
                         for i in keep_list}
        day_df = act_df[act_df.ds == date]
        keep_df = day_df.groupby('ds').count().user_id.reset_index()
        for keep_date in act_dates:
            # print keep_date
            keep_df[keep_date] = day_df[day_df['user_id'].isin(act_df[
                act_df.ds == keep_date].user_id)].count().user_id
        keep_df = keep_df.rename(columns=act_dates_dic)
        for c in act_dates_dic.values():
            keep_df[c + 'rate'] = keep_df[c] / keep_df.user_id
        dfs.append(keep_df)
    result_df = pd.concat(dfs)
    result = result_df.merge(pay_df, on='ds', how='left')
    column = ['ds', 'user_id', 'd2_keep', 'd2_keeprate', 'd3_keep',
              'd3_keeprate', 'is_pay', 'total_money']
    result = result[column]

    result.to_excel('/Users/kaiqigu/Documents/Excel/old_user_back.xlsx')
