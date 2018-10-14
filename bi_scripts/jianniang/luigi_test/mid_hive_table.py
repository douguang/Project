#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Dong Junshuang
Description : 通过hql加工的中间数据
'''
# ======mid_info_all=======
hql_info_all = '''
INSERT overwrite TABLE mid_info_all partition (ds='{date}')
SELECT now,
       user_id,
       level,
       coin,
       vip,
       gold,
       hp_bottle,
       exp,
       water,
       food,
       gas,
       medal,
       stone,
       fame,
       friendly,
       hp_train_pill,
       atk_train_pill,
       def_train_pill,
       can_use_devote,
       teach,
       teach_speedup,
       last_action_time,
       oil,
       uuid,
       account,
       platform,
       channel,
       head_icon,
       reg_time,
       faction_id,
       developed_most,
       session_id
FROM
  ( SELECT *,
           row_number() over(partition BY user_id
                             ORDER BY ds DESC) AS rn
   FROM
     ( SELECT *
      FROM raw_info
      WHERE ds = '{date}'
      UNION ALL SELECT *
      FROM mid_info_all
      WHERE ds = '{yestoday}' ) t1) t2
WHERE rn = 1
'''

# ======mid_register_account=======
hql_new_account = '''
INSERT overwrite TABLE mid_register_account partition (ds='{date}')
SELECT DISTINCT t3.account
FROM
  ( SELECT DISTINCT account
   FROM
     ( SELECT user_id
      FROM raw_registeruser
      WHERE ds='{date}' ) t1
   JOIN
     ( SELECT user_id,
              account
      FROM raw_info
      WHERE ds='{date}' ) t2 ON (t1.user_id = t2.user_id)) t3
LEFT OUTER JOIN
  ( SELECT account
   FROM mid_register_account
   WHERE ds<'{date}') t4 ON (t3.account = t4.account)
WHERE t4.account IS NULL
'''
# ======mid_active_account=======
hql_active_account = '''
INSERT overwrite TABLE mid_active_account partition (ds='{date}')
SELECT DISTINCT account
FROM
  ( SELECT user_id
   FROM raw_activeuser
   WHERE ds='{date}') t1
JOIN
  ( SELECT user_id,
           account
   FROM raw_info
   WHERE ds='{date}') t2 ON t1.user_id = t2.user_id
'''

# ======mid_assist=======
hql_assist = '''
INSERT overwrite TABLE mid_assist partition (ds='{date}')
SELECT user_id,
       name,
       server,
       ip,
       platform,
       account,
       device,
       device_mark,
       reg_time,
       act_time,
       level,
       vip,
       combat,
       first_pay_date,
       last_pay_date,
       all_pay,
       today_pay,
       free_coin,
       charge_coin,
       spend_coin,
       last_act
FROM
  ( SELECT *,
           row_number() over(partition BY user_id
                             ORDER BY ds DESC) AS rn
   FROM
     ( SELECT *
      FROM mart_assist
      WHERE ds = '{date}'
      UNION ALL SELECT *
      FROM mid_assist
      WHERE ds = '{yestoday}' ) t1) t2
WHERE rn = 1
'''

mid_dic = {
    'mid_assist': hql_assist,
    'mid_info_all': hql_info_all,
    'mid_register_account': hql_new_account,
    'mid_active_account': hql_active_account,
}
