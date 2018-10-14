#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Hu Chunlong
Description : 各项目中间数据表的sql语句
'''

# 通用语句,除超级英雄
# >>> spendlog
spendlog_sql = '''
SELECT user_id,
       sum(coin_num) AS spend_coin
FROM raw_spendlog
WHERE ds = '{date}'
GROUP BY user_id
'''
# >>> paylog
paylog_sql = '''
SELECT t1.user_id,
       all_pay,
       first_pay_date,
       last_pay_date,
       today_pay
FROM
  ( SELECT user_id,
           sum(order_money) AS all_pay,
           min(to_date(order_time)) AS first_pay_date,
           max(to_date(order_time)) AS last_pay_date
   FROM raw_paylog
   WHERE ds <= '{date}'
   GROUP BY user_id ) t1
LEFT OUTER JOIN
  ( SELECT user_id,
           sum(order_money) AS today_pay
   FROM raw_paylog
   WHERE ds = '{date}'
   GROUP BY user_id ) t2 ON t1.user_id = t2.user_id
'''
# ======================武娘======================
# >>> dancer_raw_info <<<
dancer_raw_info_sql = '''
SELECT user_id,
       account,
       name,
       device_mark,
       to_date(reg_time) as reg_time,
       to_date(act_time) as act_time,
       level,
       vip,
       combat,
       regist_ip as ip
FROM parse_info
WHERE ds = '{date}'
'''
# >>> dancer_parse_actionlog_all <<<
dancer_parse_actionlog_all_sql = '''
SELECT t1.user_id,
       server,
       platform,
       a_typ as last_act
FROM
  (SELECT user_id,
          max(log_t) AS log_t
   FROM parse_actionlog
   WHERE ds = '{date}'
   GROUP BY user_id) t1
LEFT OUTER JOIN
  (SELECT user_id,
          server,
          platform,
          log_t,
          a_typ
   FROM parse_actionlog
   WHERE ds = '{date}') t2 ON t1.user_id = t2.user_id
AND t1.log_t = t2.log_t
'''

dancer_parse_actionlog_all_2_sql = '''
SELECT user_id,
       sum(freemoney_diff) AS free_coin,
       sum(money_diff) AS charge_coin
FROM parse_actionlog
WHERE ds = '{date}'
 AND (freemoney_diff > 0
      OR money_diff >0)
GROUP BY user_id
'''
# >>> dancer_nginx <<<
dancer_nginx_sql = '''
SELECT DISTINCT user_token AS user_id,
                device
FROM raw_nginx
WHERE ds = '{date}'
  AND device != ''
  AND user_token != ''
'''

# ======================三国&器灵======================
# >>> sanguo_raw_info <<<
sanguo_raw_info_sql = '''
SELECT user_id,
       account,
       name,
       platform,
       device as device_mark,
       to_date(reg_time) as reg_time,
       to_date(act_time) as act_time,
       level,
       vip,
       combat
FROM raw_info
WHERE ds = '{date}'
'''
# >>> sanguo_parse_actionlog <<<
sanguo_parse_actionlog_sql = '''
SELECT t1.user_id,
       server,
       a_typ as last_act
FROM
  (SELECT user_id,
          max(log_t) AS log_t
   FROM parse_actionlog
   WHERE ds = '{date}'
   GROUP BY user_id) t1
LEFT OUTER JOIN
  (SELECT user_id,
          server,
          log_t,
          a_typ
   FROM parse_actionlog
   WHERE ds = '{date}') t2 ON t1.user_id = t2.user_id
AND t1.log_t = t2.log_t
'''
sanguo_parse_actionlog_2_sql = '''
SELECT user_id,
       sum(freemoney_diff) AS free_coin,
       sum(money_diff) AS charge_coin
FROM parse_actionlog
WHERE ds = '{date}'
 AND (freemoney_diff > 0
      OR money_diff >0)
GROUP BY user_id
'''
# >>> sanguo_nginx <<<
sanguo_nginx_sql = '''
SELECT DISTINCT user_token AS user_id,
                devicename AS device,
                ip
FROM raw_nginx
WHERE ds = '{date}'
  AND devicename != ''
  AND user_token != ''
  AND ip != ''
'''

# ======================超级英雄======================
# >>> superhero_raw_info <<<
superhero_raw_info_sql = '''
SELECT uid as user_id,
       reverse(substr(reverse(uid),8)) AS server,
       account,
       nick as name,
       platform_2 as platform,
       device as device_mark,
       to_date(create_time) as reg_time,
       to_date(fresh_time) as act_time,
       level,
       vip_level as vip,
       zhandouli as combat
FROM raw_info
WHERE ds = '{date}'
'''
# >>> superhero_parse_actionlog <<<
superhero_parse_actionlog_sql = '''
SELECT t1.uid  as user_id,
       action as last_act
FROM
  (SELECT uid,
          max(stmp) AS stmp
   FROM raw_action_log
   WHERE ds = '{date}'
   GROUP BY uid) t1
LEFT OUTER JOIN
  (SELECT uid,
          stmp,
          action
   FROM raw_action_log
   WHERE ds = '{date}') t2 ON t1.uid = t2.uid
AND t1.stmp = t2.stmp
'''
superhero_paylog_sql = '''
SELECT t1.uid as user_id,
       all_pay,
       first_pay_date,
       last_pay_date,
       today_pay,
       charge_coin
FROM
  ( SELECT uid,
           sum(order_money) AS all_pay,
           min(to_date(order_time)) AS first_pay_date,
           max(to_date(order_time)) AS last_pay_date
   FROM raw_paylog
   WHERE ds <= '{date}'
   GROUP BY uid ) t1
LEFT OUTER JOIN
  ( SELECT uid,
           sum(order_money) AS today_pay,
           sum(order_coin) + sum(gift_coin) AS charge_coin
   FROM raw_paylog
   WHERE ds = '{date}'
   GROUP BY uid ) t2 ON t1.uid = t2.uid
'''

# >>> superhero_spendlog <<<
superhero_spendlog_sql = '''
SELECT uid AS user_id,
       sum(coin_num) AS spend_coin
FROM raw_spendlog
WHERE ds = '{date}'
GROUP BY uid
'''
# >>> superhero_nginx <<<
# superhero_nginx_sql = '''
# SELECT DISTINCT user_token AS user_id,
#                 devicename AS device,
#                 ip
# FROM raw_nginx
# WHERE ds = '{date}'
#   AND devicename != ''
#   AND user_token != ''
#   AND ip != ''
# '''
# >>> superhero_spendlog <<<

# ======================pandas常用语句======================
"""
# 填充缺失值的方法
dataframe.fillna({'code': 'code', 'date': 'date'})
# df中增加日期的方法
new_user_df['today'] = pd.Timestamp('{date}')
'''
# 日期格式的数据需要使用to_datatime函数进行处理
pd.Period(date)
ori_df['reg_time'] = pd.to_datetime({loandata['issue_d'],)
# 计算各列数据总和并作为新列添加到末尾
df['Col_sum'] = df.apply(lambda x: x.sum(), axis=1)
# 计算各行数据总和并作为新行添加到末尾
df.loc['Row_sum'] = df.apply(lambda x: x.sum())
# 把数据按列分割
date = df.pop('date')
# 用.insert()方法进行插入列
df.insert(0, 'date', date)
df['winter'] = winter
# 删除pandas DataFrame的某一/几列
DF = DF.drop('column_name', 1)
DF.drop('column_name', axis=1, inplace=True)
DF.drop([DF.columns[[0, 1, 3]]], axis=1, inplace=True)
# 如何对dataFrame的两列相乘后的结果作为新列添加进Dataframe中
import pandas as pd
a = [[1, 2, 3], [4, 5, 6]]
b = pd.DataFrame(a)
c = b[0] * b[1]
# df.insert(idx, col_name, value)
# insert 三个参数，插到第几列，该列列名，值
b.insert(3, 3, c)
'''
ori_df['ds'] = date  # 测试时是>>>2017-01-10<<<格式
ori_df['ds'] = ori_df['ds'].astype('datetime64')
ori_df['act_time'] = ori_df['act_time'].astype('datetime64')
# 用户流失天数
ori_df['loss_days'] = (
    ori_df['ds'] - ori_df['act_time']) / np.timedelta64(1, 'D')
# 用户未充值天数

# 当日新增充值用户
len(ori_df[ori_df['first_pay_date'] == ori_df['ds']])
# 新注册用户的df
new_user_df = DataFrame(ori_df['user_id'][ori_df['reg_time'] == ori_df['ds']], columns=['user_id'])
new_user_df['is_new_user']=1
# 新充值用户的df

# 新充值用户的df

# 当日充值用户的df

# 当日活跃用户的df
# # 计算用户流失天数
# ori_df['loss_days'] = (
#     ori_df['today'] - ori_df['act_time']) / np.timedelta64(1, 'D')
# # 用户未充值天数
# ori_df['loss_pay_days'] = (
#     ori_df['today'] - ori_df['last_pay_date']) / np.timedelta64(1, 'D')
# # 新注册用户的df
# reg_df = DataFrame(ori_df['user_id'][ori_df['reg_time'] == ori_df[
#     'today']], columns=['user_id'])
# reg_df['reg_user'] = 1
# # 新充值用户的df
# new_pay_df = DataFrame(ori_df['user_id'][ori_df['first_pay_date'] == ori_df[
#     'today']], columns=['user_id'])
# new_pay_df['new_pay_user'] = 1
# # 当日充值用户的df
# today_pay_df = DataFrame(ori_df['user_id'][ori_df['last_pay_date'] == ori_df[
#     'today']], columns=['user_id'])
# today_pay_df['today_pay_user'] = 1
# # 充值用户的df
# pay_df = DataFrame(ori_df['user_id'][ori_df['all_pay'].notnull()])
# pay_df['pay_user'] = 1
# # 当日活跃用户的df
# today_act_df = DataFrame(ori_df['user_id'][ori_df['last_pay_date'] == ori_df[
#     'today']], columns=['user_id'])
# today_act_df['today_act_user'] = 1
# # 合并所有数据
# result_df = ori_df.merge(reg_df, on='user_id', how='left').merge(new_pay_df, on='user_id', how='left').merge(
# today_pay_df, on='user_id', how='left').merge(today_act_df,
# on='user_id', how='left').merge(pay_df, on='user_id', how='left')
"""
