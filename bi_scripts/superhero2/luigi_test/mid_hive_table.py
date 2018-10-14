#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Dong Junshuang
Description : 通过hql加工的中间数据
'''
# ======mid_info_all=======
hql_info_all = '''
INSERT overwrite TABLE mid_info_all partition (ds='{date}')
SELECT uuid,
       account,
       user_id,
       name,
       level,
       act_time,
       dark_coin,
       challenge,
       silver_ticket,
       reg_time,
       guide,
       platform,
       vip,
       server,
       son_server,
       combat,
       device_mark,
       diamond_free,
       diamond_charge,
       create_date,
       coin,
       silver,
       diamond_ticket,
       appid,
       ip,
       account_reg
FROM
  ( SELECT *,
           row_number() over(partition BY user_id
                             ORDER BY ds DESC) AS rn
   FROM
     ( SELECT *
      FROM parse_info
      WHERE ds = '{date}'
      UNION ALL SELECT *
      FROM mid_info_all
      WHERE ds = '{yestoday}' ) t1) t2
WHERE rn = 1
'''

# ======mid_paylog_all=======
hql_paylog_all = '''
INSERT overwrite TABLE mid_paylog_all partition (ds='{date}')
SELECT order_id,
       level,
       old_coin,
       gift_coin,
       order_coin,
       order_money,
       order_rmb,
       is_double,
       money_type,
       order_time,
       platform_2,
       conf_id,
       platconf_id,
       platorder_id,
       uid,
       plat_id,
       admin,
       reason
FROM
  ( SELECT *,
           row_number() over(partition BY order_id
                             ORDER BY ds DESC) AS rn
   FROM
     ( SELECT *
      FROM raw_paylog
      WHERE ds = '{date}'
      UNION ALL SELECT *
      FROM mid_paylog_all
      WHERE ds = '{yestoday}' ) t1) t2
WHERE rn = 1
'''
# ======mid_active_account=======
hql_new_account = '''
INSERT overwrite TABLE mid_new_account partition (ds='{date}')
SELECT DISTINCT account,
                platform,
                account_reg
FROM parse_info
WHERE ds= '{date}'
  AND regexp_replace(substr(account_reg,1,10),'-','') = '{date}'
'''

mid_dic = {
    'mid_info_all': hql_info_all,
    'mid_paylog_all': hql_paylog_all,
    'mid_new_account': hql_new_account,
}


# # ======mid_info_all=======
foreign_hql_info_all = '''
INSERT overwrite TABLE mid_info_all partition (ds='{date}')
SELECT uid,
       account,
       nick,
       platform_2,
       device,
       create_time,
       fresh_time,
       vip_level,
       level,
       zuanshi,
       gold_coin,
       silver_coin,
       only_access,
       file_date
FROM
  ( SELECT *,
           row_number() over(partition BY uid
                             ORDER BY ds DESC) AS rn
   FROM
     ( SELECT *
      FROM parse_info
      WHERE ds = '{date}'
      UNION ALL SELECT *
      FROM mid_info_all
      WHERE ds = '{yestoday}' ) t1) t2
WHERE rn = 1
'''
# ======mid_paylog_all=======
foreign_hql_paylog_all = '''
INSERT overwrite TABLE mid_paylog_all partition (ds='{date}')
SELECT order_id,
       level,
       old_coin,
       gift_coin,
       order_coin,
       order_money,
       order_rmb,
       is_double,
       money_type,
       order_time,
       platform_2,
       conf_id,
       platconf_id,
       platorder_id,
       uid,
       plat_id,
       admin,
       reason
FROM
  ( SELECT *,
           row_number() over(partition BY order_id
                             ORDER BY ds DESC) AS rn
   FROM
     ( SELECT *
      FROM raw_paylog
      WHERE ds = '{date}'
      UNION ALL SELECT *
      FROM mid_paylog_all
      WHERE ds = '{yestoday}' ) t1) t2
WHERE rn = 1
'''
# ======mid_new_account=======
foreign_hql_new_account = '''
INSERT overwrite TABLE mid_new_account partition (ds='{date}')
SELECT a.account,
       a.uid,
       a.plat,
       a.platform_2,
       a.server
FROM
  (SELECT account,
          uid,
          substr(uid,1,1) plat,
          platform_2,
          reverse(substr(reverse(uid), 8)) AS server
   FROM parse_info
   WHERE ds= '{date}')a LEFT semi
JOIN
  (SELECT uid
   FROM raw_reg
   WHERE ds= '{date}')b ON a.uid=b.uid
WHERE a.account NOT IN
    (SELECT account
     FROM mid_info_all
     WHERE ds= '{yestoday}')
'''

foreign_mid_dic = {
    'mid_info_all': foreign_hql_info_all,
    'mid_paylog_all': foreign_hql_paylog_all,
    'mid_new_account': foreign_hql_new_account,
}
