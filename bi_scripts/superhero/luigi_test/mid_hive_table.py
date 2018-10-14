#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Dong Junshuang
Description : 通过hql加工的中间数据
'''
# ======mid_info_all=======
hql_info_all = '''
INSERT overwrite TABLE mid_info_all partition (ds='{date}')
SELECT uid,
       account,
       nick,
       platform_2,
       device,
       create_time,
       fresh_time,
       vip_level,
       LEVEL,
       zhandouli,
       food,
       metal,
       energy,
       nengjing,
       zuanshi,
       qiangnengzhichen,
       chaonengzhichen,
       xingdongli,
       xingling,
       jinbi,
       lianjingshi,
       shenen,
       gaojishenen,
       gaojinengjing,
       jingjichangdianshu
FROM
  ( SELECT *,
           row_number() over(partition BY uid
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

# ======mid_paylog_all=======
hql_paylog_all = '''
INSERT overwrite TABLE mid_paylog_all partition (ds='{date}')
SELECT order_id,
       ADMIN,
       gift_coin,
       LEVEL,
       old_coin,
       order_coin,
       order_money,
       order_time,
       platform_2,
       product_id,
       raw_data,
       reason,
       scheme_id,
       uid,
       pay_pt,
       pay_pt_2
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
   FROM raw_info
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

# # ======mid_gs=======
# # 测试用户根据admin_test的充值金额/admin_test用户的历史充值总额的50%以上,后续的测试用户由运营提供UID
# hql_gs = '''
# INSERT overwrite TABLE mid_gs partition(ds='20170502')
# with total_pay as (
#     select uid, platform_2, order_money from total_paylog where order_time <= '2017-02-21 59:59:59'
#     union all
#     select uid, platform_2, order_money from raw_paylog where ds >= '20170222'
# )
# select uid from (
#     select a.uid, total, sum_money, sum_money / total as rate from (
#         select uid, sum(order_money) total from total_pay  group by uid
#     )a join(
#         select uid, sum(order_money) sum_money from total_pay where platform_2='admin_test' group by uid
#     )b on a.uid=b.uid
# )c where rate >= 0.50
# '''

mid_dic = {
    'mid_info_all': hql_info_all,
    'mid_paylog_all': hql_paylog_all,
    'mid_new_account': hql_new_account,
    # 'mid_gs': hql_gs,
}

# ======mid_info_all=======
bi_hql_info_all = '''
INSERT overwrite TABLE mid_info_all partition (ds='{date}')
SELECT uid,
       account,
       nick,
       platform_2,
       device,
       create_time,
       fresh_time,
       vip_level,
       LEVEL,
       zhandouli,
       food,
       metal,
       energy,
       nengjing,
       zuanshi,
       qiangnengzhichen,
       chaonengzhichen,
       xingdongli,
       xingling,
       jinbi,
       lianjingshi,
       shenen,
       gaojishenen,
       gaojinengjing,
       jingjichangdianshu,
       file_date,
       bundle_id
FROM
  ( SELECT *,
           row_number() over(partition BY uid
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

bi_mid_dic = {
    'mid_info_all': bi_hql_info_all,
    'mid_paylog_all': hql_paylog_all,
    'mid_new_account': hql_new_account,
    # 'mid_gs': hql_gs,
}

# ======mid_info_all=======
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
       LEVEL,
       zhandouli,
       food,
       metal,
       energy,
       nengjing,
       zuanshi,
       qiangnengzhichen,
       chaonengzhichen,
       xingdongli,
       xingling,
       jinbi,
       lianjingshi,
       shenen,
       gaojishenen,
       gaojinengjing,
       jingjichangdianshu
FROM
  ( SELECT *,
           row_number() over(partition BY uid
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
# ======mid_paylog_all=======
foreign_hql_paylog_all = '''
INSERT overwrite TABLE mid_paylog_all partition (ds='{date}')
SELECT order_id,
       ADMIN,
       gift_coin,
       LEVEL,
       old_coin,
       order_coin,
       order_vnd,
       order_money,
       order_time,
       platform_2,
       product_id,
       raw_data,
       reason,
       scheme_id,
       uid
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
   FROM raw_info
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
    # 'mid_gs': foreign_hql_gs,
}

# ======mid_info_all=======
self_hql_info_all = '''
INSERT overwrite TABLE mid_info_all partition (ds='{date}')
SELECT uid,
       account,
       nick,
       platform_2,
       device,
       create_time,
       fresh_time,
       vip_level,
       LEVEL,
       zhandouli,
       food,
       metal,
       energy,
       nengjing,
       zuanshi,
       qiangnengzhichen,
       chaonengzhichen,
       xingdongli,
       xingling,
       jinbi,
       lianjingshi,
       shenen,
       gaojishenen,
       gaojinengjing,
       jingjichangdianshu,
       file_date
FROM
  ( SELECT *,
           row_number() over(partition BY uid
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

# ======mid_paylog_all=======
self_hql_paylog_all = '''
INSERT overwrite TABLE mid_paylog_all partition (ds='{date}')
SELECT order_id,
       ADMIN,
       gift_coin,
       LEVEL,
       old_coin,
       order_coin,
       order_money,
       order_rmb,
       order_time,
       platform_2,
       product_id,
       raw_data,
       reason,
       scheme_id,
       uid,
       plat_account,
       vip_exp
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
self_hql_new_account = '''
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
   FROM raw_info
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
self_mid_dic = {
    'mid_info_all': self_hql_info_all,
    # 'mid_paylog_all': self_hql_paylog_all,
    'mid_new_account': self_hql_new_account,
}


mul_hql_info_all = '''
INSERT overwrite TABLE mid_info_all partition (ds='{date}')
SELECT uid,
       account,
       nick,
       platform_2,
       device,
       create_time,
       fresh_time,
       vip_level,
       LEVEL,
       zhandouli,
       food,
       metal,
       energy,
       nengjing,
       zuanshi,
       qiangnengzhichen,
       chaonengzhichen,
       xingdongli,
       xingling,
       jinbi,
       lianjingshi,
       shenen,
       gaojishenen,
       gaojinengjing,
       jingjichangdianshu,
       file_date,
       bundle_id,
       ip
FROM
  ( SELECT *,
           row_number() over(partition BY uid
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

mul_mid_dic = {
    'mid_info_all': mul_hql_info_all,
    'mid_paylog_all': hql_paylog_all,
    'mid_new_account': hql_new_account,
    # 'mid_gs': hql_gs,
}

vt_hql_info_all = '''
INSERT overwrite TABLE mid_info_all partition (ds='{date}')
SELECT uid,
       account,
       nick,
       platform_2,
       device,
       create_time,
       fresh_time,
       vip_level,
       LEVEL,
       zhandouli,
       food,
       metal,
       energy,
       nengjing,
       zuanshi,
       qiangnengzhichen,
       chaonengzhichen,
       xingdongli,
       xingling,
       jinbi,
       lianjingshi,
       shenen,
       gaojishenen,
       gaojinengjing,
       jingjichangdianshu,
       file_date,
       ip
FROM
  ( SELECT *,
           row_number() over(partition BY uid
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

vt_mid_dic = {
    'mid_info_all': vt_hql_info_all,
    'mid_paylog_all': hql_paylog_all,
    'mid_new_account': hql_new_account,
    # 'mid_gs': hql_gs,
}
