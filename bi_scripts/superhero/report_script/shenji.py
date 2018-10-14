#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Dong Junshuang
Description : 超级英雄-审计数据(pub、ios、qiku)
'''
import settings_dev
import pandas as pd
from utils import hqls_to_dfs, hql_to_df, ds_add


def get_sql(start_date, end_date):
    # 1.活跃用户数统计
    # 月活跃用户数
    act_sql = '''
    SELECT substr(ds,1,6) ds_str,
           count(distinct uid) act_num
    FROM raw_info
    WHERE ds >='{start_date}'
      AND ds <='{end_date}'
    GROUP BY substr(ds,1,6)
    ORDER BY substr(ds,1,6)
    '''.format(start_date=start_date, end_date=end_date)
    # 注册用户数
    reg_sql = '''
    SELECT substr(ds,1,6) ds_str,
           count(DISTINCT uid) reg_num
    FROM raw_reg
    WHERE ds >='{start_date}'
      AND ds <='{end_date}'
    GROUP BY substr(ds,1,6)
    ORDER BY substr(ds,1,6)
    '''.format(start_date=start_date, end_date=end_date)
    # 新增设备数
    device_sql = '''
    SELECT substr(ds, 1, 6) AS ds_str, COUNT(DISTINCT device) AS new_device
    FROM raw_info
    WHERE ds >= '{start_date}'
        AND ds <= '{end_date}'
        AND regexp_replace(to_date(create_time), '-', '') >= '{start_date}'
        AND regexp_replace(to_date(create_time), '-', '') <= '{end_date}'
    GROUP BY substr(ds, 1, 6)
    ORDER BY substr(ds, 1, 6)
    '''.format(start_date=start_date, end_date=end_date, date_before=ds_add(start_date, -1))
    # 新增账号数
    account_sql = '''
    SELECT substr(ds, 1, 6) AS ds_str, COUNT(DISTINCT account) AS new_account
    FROM raw_info
    WHERE ds >= '{start_date}'
        AND ds <= '{end_date}'
        AND regexp_replace(to_date(create_time), '-', '') >= '{start_date}'
        AND regexp_replace(to_date(create_time), '-', '') <= '{end_date}'
        AND account NOT IN (SELECT DISTINCT account
            FROM mid_info_all
            WHERE ds = '{date_before}')
    GROUP BY substr(ds, 1, 6)
    ORDER BY substr(ds, 1, 6)
    '''.format(start_date=start_date, end_date=end_date, date_before=ds_add(start_date, -1))
    # 日均活跃dau
    dau_sql = '''
    SELECT substr(ds, 1, 6) AS ds_str, CAST(SUM(dau) / COUNT(ds) AS decimal(38, 0)) AS average_dau
    FROM (SELECT ds, COUNT(DISTINCT uid) AS dau
        FROM raw_info
        WHERE ds >= '{start_date}'
            AND ds <= '{end_date}'
        GROUP BY ds
        ) t1
    GROUP BY substr(ds, 1, 6)
    ORDER BY substr(ds, 1, 6)
    '''.format(start_date=start_date, end_date=end_date)
    # 充值人数，充值金额
    pay_sql = '''
    SELECT substr(ds,1,6) ds_str,
           count(DISTINCT uid) pay_num,
           sum(order_money) pay_money
    FROM raw_paylog
    WHERE ds >='{start_date}'
      AND ds <='{end_date}'
      AND platform_2 <> 'admin_test'
    GROUP BY substr(ds,1,6)
    ORDER BY substr(ds,1,6)
    '''.format(start_date=start_date, end_date=end_date)
    # 2.道具销售
    goods_sql = '''
    SELECT goods_type,
           sum(coin_num) sum_coin
    FROM raw_spendlog
    WHERE ds>='{start_date}'
      AND ds<='{end_date}'
      -- AND goods_type IN ('gacha.do_reward_gacha',
      --                    'shop.buy',
      --                    'magic_school.open_contract',
      --                    'roulette.open_roulette10',
      --                    'one_piece.open_roulette10' )
    GROUP BY goods_type
    '''.format(start_date=start_date, end_date=end_date)
    # 3.充值时段
    times_ivtl_sql = '''
    SELECT ivtl,
           count(DISTINCT uid) pay_user_num,
           count(order_id) pay_times,
           sum(order_money) pay_money
    FROM
      (SELECT uid,
              order_id,
              order_money,
              order_time,
              CASE
                  WHEN tt >=7 AND tt <12 THEN 'a'
                  WHEN tt >=12 AND tt <17 THEN 'b'
                  WHEN tt >=17 AND tt <23 THEN 'c'
                  WHEN tt >=23 AND tt <= 24 THEN 'd'
                  WHEN tt >=0 AND tt < 7 THEN 'd'
              END ivtl
       FROM
         (SELECT uid,
                 order_id,
                 order_money,
                 order_time,
                 hour(order_time) tt
          FROM raw_paylog
          WHERE ds>='{start_date}'
            AND ds<='{end_date}'
            AND platform_2 <> 'admin_test')a)b
    GROUP BY ivtl
    '''.format(start_date=start_date, end_date=end_date)
    # 审计 - 玩家充值明细
    pay_detail_sql = '''
    SELECT a.uid,
           a.sum_pay,
           a.max_pay,
           a.pay_num,
           nvl(c.login_day_num,0) login_day_num,
           a.pay_day_num,
           b.create_time,
           b.last_login_time,
           a.first_order_time,
           a.last_order_time,
           a.level,
           a.sum_coin,
           b.day_coin_num,
           b.platform_2
    FROM
      -- (SELECT uid,
      --         sum(order_money) sum_pay,
      --         sum(order_coin) sum_coin,
      --         count(order_id) pay_num,
      --         max(LEVEL) LEVEL,
      --                    count(DISTINCT to_date(order_time)) pay_day_num,
      --                    min(order_time) first_order_time,
      --                    max(order_time) last_order_time
      --  FROM mid_paylog_all
      --  WHERE ds ='{end_date}'
      --  GROUP BY uid )a
      (SELECT uid,
              sum(order_money) sum_pay,
              sum(order_coin) sum_coin,
              max(order_money) as max_pay,
              count(order_id) pay_num,
              max(LEVEL) LEVEL,
                         count(DISTINCT to_date(order_time)) pay_day_num,
                         min(order_time) first_order_time,
                         max(order_time) last_order_time
       FROM raw_paylog
       WHERE ds >='{start_date}'
       and ds <='{end_date}'
      AND platform_2 <> 'admin_test'
       GROUP BY uid)a
    JOIN
      (SELECT uid,
              platform_2,
              create_time,
              last_login_time,
              day_coin_num
       FROM
         (SELECT uid,
                 platform_2,
                 create_time,
                 fresh_time AS last_login_time,
                 zuanshi AS day_coin_num,
                 row_number() over(partition BY uid
                                   ORDER BY ds DESC) rn
          FROM raw_info
          WHERE ds >='{start_date}'
            AND ds <='{end_date}')d
       WHERE rn =1)b ON a.uid = b.uid
    LEFT JOIN
      ( SELECT uid,
               count(ds) login_day_num
       FROM raw_act
       WHERE ds >='{start_date}'
       and ds <='{end_date}'
       GROUP BY uid )c ON b.uid = c.uid
    ORDER BY a.sum_pay DESC LIMIT 3000
    '''.format(start_date=start_date, end_date=end_date)
    # 审计 - 充值区间
    pay_ivtl_sql = '''
    SELECT itvl,
           count(uid) user_num,
           sum(pay_times) sum_times,
           sum(sum_pay) sum_pay,
           avg(LEVEL) avg_level
    FROM
      (SELECT uid,
              sum_pay,
              LEVEL,
              pay_times,
              CASE
                  WHEN sum_pay < 1000 THEN '1000'
                  WHEN sum_pay >= 1000 AND sum_pay < 3000 THEN '1000_3000'
                  WHEN sum_pay >= 3000 AND sum_pay < 5000 THEN '3000_5000'
                  WHEN sum_pay >= 5000 AND sum_pay < 10000 THEN '5000_10000'
                  WHEN sum_pay > 10000 THEN '10001'
              END AS itvl
       FROM
         (SELECT uid,
                 sum(order_money) sum_pay,
                 max(LEVEL) LEVEL,
                            count(order_id) pay_times
          FROM raw_paylog
          WHERE ds>='{start_date}'
            AND ds<='{end_date}'
            AND platform_2 <> 'admin_test'
          GROUP BY uid)a)b
    WHERE itvl IS NOT NULL
    GROUP BY itvl
    '''.format(start_date=start_date, end_date=end_date)
    # 查询单次充值2000以上用户
    pay_2000_sql = '''
    SELECT DISTINCT uid
    FROM raw_paylog
    WHERE ds >='{start_date}'
      AND ds <='{end_date}'
      AND platform_2 <> 'admin_test'
      AND order_money >2000
    '''.format(start_date=start_date, end_date=end_date)
    # # 14年-15年pub、ios、七酷的收入
    # '''
    # select substr(order_time,1,7),sum(order_money)  from total_paylog
    # where platform_2 <> 'admin_test'
    # group by substr(order_time,1,7)
    # order by substr(order_time,1,7)
    # '''

    # 统计同个游戏内充值次数大于1次的玩家账号（等于1次的不统计）
    pay_times_sql = '''
        SELECT t1.uid, platform_2, times, pay
        FROM (SELECT uid, COUNT(uid) AS times, SUM(order_money) AS pay
            FROM raw_paylog
            WHERE ds >= '{start_date}'
                AND ds <= '{end_date}'
                AND platform_2 <> 'admin_test'
            GROUP BY uid
            HAVING times > 1
            ) t1
            INNER JOIN (SELECT uid, platform_2
                FROM mid_info_all
                WHERE ds = '{end_date}'
                ) t2 ON t1.uid = t2.uid
    '''.format(start_date=start_date, end_date=end_date)
    return act_sql, reg_sql, device_sql, account_sql, dau_sql, pay_sql, goods_sql, times_ivtl_sql, pay_detail_sql, pay_ivtl_sql, pay_2000_sql, pay_times_sql


if __name__ == '__main__':
    start_date = '20160501'
    end_date = '20170930'
    settings_dev.set_env('superhero_bi')
    act_sql, reg_sql, device_sql, account_sql, dau_sql, pay_sql, goods_sql, times_ivtl_sql, pay_detail_sql, pay_ivtl_sql, pay_2000_sql, pay_times_sql = get_sql(
        start_date, end_date)

    # 单月统计
    # act_df = hql_to_df(act_sql)
    # reg_df = hql_to_df(reg_sql)
    # device_df = hql_to_df(device_sql)
    # account_df = hql_to_df(account_sql)
    # dau_df = hql_to_df(dau_sql)
    # pay_df = hql_to_df(pay_sql)
    # goods_df = hql_to_df(goods_sql)
    # 历史统计
    times_ivtl_df = hql_to_df(times_ivtl_sql)
    pay_detail_df = hql_to_df(pay_detail_sql)
    pay_ivtl_df = hql_to_df(pay_ivtl_sql)
    pay_2000_df = hql_to_df(pay_2000_sql)
    pay_times_df = hql_to_df(pay_times_sql)

    settings_dev.set_env('superhero_qiku')

    # 单月统计
    # q_act_df = hql_to_df(act_sql)
    # q_reg_df = hql_to_df(reg_sql)
    # q_device_df = hql_to_df(device_sql)
    # q_account_df = hql_to_df(account_sql)
    # q_dau_df = hql_to_df(dau_sql)
    # q_pay_df = hql_to_df(pay_sql)
    # q_goods_df = hql_to_df(goods_sql)
    # 历史统计
    q_times_ivtl_df = hql_to_df(times_ivtl_sql)
    q_pay_detail_df = hql_to_df(pay_detail_sql)
    q_pay_ivtl_df = hql_to_df(pay_ivtl_sql)
    q_pay_2000_df = hql_to_df(pay_2000_sql)
    q_pay_times_df = hql_to_df(pay_times_sql)

    # 单月统计
    # act_df_result = pd.concat(
    #     [act_df, q_act_df]).groupby('ds_str').sum().reset_index()
    # reg_df_result = pd.concat(
    #     [reg_df, q_reg_df]).groupby('ds_str').sum().reset_index()
    # device_df_result = pd.concat(
    #     [device_df, q_device_df]).groupby('ds_str').sum().reset_index()
    # account_df_result = pd.concat(
    #     [account_df, q_account_df]).groupby('ds_str').sum().reset_index()
    # dau_df_result = pd.concat(
    #     [dau_df, q_dau_df]).groupby('ds_str').sum().reset_index()
    # pay_df_result = pd.concat(
    #     [pay_df, q_pay_df]).groupby('ds_str').sum().reset_index()
    # goods_df_result = pd.concat(
    #     [goods_df, q_goods_df]).groupby('goods_type').sum().reset_index()
    # goods_df_result = goods_df_result.sort_values(
    #     by='sum_coin', ascending=False)[:5]
    # act_user_result = (act_df_result.merge(
    #     reg_df_result, on='ds_str', how='outer').merge(
    #     device_df_result, on='ds_str', how='outer').merge(
    #     account_df_result, on='ds_str', how='outer').merge(
    #     dau_df_result, on='ds_str', how='outer').merge(
    #     pay_df_result, on='ds_str', how='outer')[
    #                        ['ds_str', 'reg_num', 'new_device', 'new_account', 'average_dau', 'act_num', 'pay_num', 'pay_money']])
    # act_user_result.to_excel(
    #     r'E:\Data\output\superhero\act_user_result.xlsx')
    # goods_df_result.to_excel(
    #     r'E:\Data\output\superhero\goods_df_result.xlsx')

    # 历史统计
    times_ivtl_df_result = pd.concat(
        [times_ivtl_df, q_times_ivtl_df]).groupby('ivtl').sum().reset_index()
    pay_detail_df_result = pd.concat([pay_detail_df, q_pay_detail_df])
    pay_detail_df_result = pay_detail_df_result.sort_values(
        by='sum_pay', ascending=False)[:3000]
    pay_ivtl_df_result = pd.concat(
        [pay_ivtl_df, q_pay_ivtl_df]).groupby('itvl').sum().reset_index()
    pay_ivtl_df_result['avg_level_data'] = pay_ivtl_df_result['avg_level'] * 1.0 / 2
    pay_2000_df_result = pd.concat([pay_2000_df, q_pay_2000_df])
    pay_times_df_result = pd.concat([pay_times_df, q_pay_times_df])
    pay_times_df_result = pay_times_df_result.sort_values(by='times', ascending=False)
    times_ivtl_df_result.to_excel(
        r'E:\Data\output\superhero\times_ivtl_df_result.xlsx')
    pay_detail_df_result.to_excel(
        r'E:\Data\output\superhero\pay_detail_df_result.xlsx')
    pay_ivtl_df_result.to_excel(
        r'E:\Data\output\superhero\pay_ivtl_df_result.xlsx')
    pay_2000_df_result.to_excel(
        r'E:\Data\output\superhero\pay_2000_df_result.xlsx')
    pay_times_df_result.to_excel(
        r'E:\Data\output\superhero\pay_times_df_result.xlsx')