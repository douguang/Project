#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Dong Junshuang
Description : 通过hql加工的中间数据
'''
# ======mid_info_all=======
hql_info_all = '''
INSERT overwrite table mid_info_all partition (ds='{date}')
SELECT user_id,
       account,
       name,
       reg_time,
       act_time,
       LEVEL,
       vip,
       free_coin,
       charge_coin,
       gold,
       energy,
       cmdr_energy,
       honor,
       combat,
       guide,
       max_stage,
       item_dict,
       card_dict,
       equip_dict,
       combiner_dict,
       once_reward,
       card_assistant,
       combiner_in_use,
       card_assis_active,
       chips,
       chip_pos,
       equip_pos,
       device_mark,
       emblems,
       achievements,
       regist_ip,
       contact_skill,
       stone_dict,
       books_dict,
       reward_dict,
       acupoint_dict,
       appid,
       regist_time,
       pet_dict
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

# ======武娘多语言的mid_info_all=======
dancer_mul_hql_info_all = '''
INSERT overwrite table mid_info_all partition (ds='{date}')
SELECT user_id,
       account,
       name,
       reg_time,
       act_time,
       LEVEL,
       vip,
       free_coin,
       charge_coin,
       gold,
       energy,
       cmdr_energy,
       honor,
       combat,
       guide,
       max_stage,
       item_dict,
       card_dict,
       equip_dict,
       combiner_dict,
       once_reward,
       card_assistant,
       combiner_in_use,
       card_assis_active,
       chips,
       chip_pos,
       equip_pos,
       device_mark,
       emblems,
       achievements,
       regist_ip,
       contact_skill,
       stone_dict,
       books_dict,
       reward_dict,
       appid,
       regist_time,
       language_sort,
       register_lan_sort
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
# ======武娘变态服的mid_info_all=======
dancer_bt_hql_info_all = '''
INSERT overwrite table mid_info_all partition (ds='{date}')
SELECT user_id,
       account,
       name,
       reg_time,
       act_time,
       LEVEL,
       vip,
       free_coin,
       charge_coin,
       gold,
       energy,
       cmdr_energy,
       honor,
       combat,
       guide,
       max_stage,
       item_dict,
       card_dict,
       equip_dict,
       combiner_dict,
       once_reward,
       card_assistant,
       combiner_in_use,
       card_assis_active,
       chips,
       chip_pos,
       equip_pos,
       device_mark,
       emblems,
       achievements,
       regist_ip,
       contact_skill,
       stone_dict,
       books_dict,
       reward_dict,
       acupoint_dict,
       appid,
       regist_time,
       secre_dict,
       pet_dict
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

# ======武娘国服的mid_info_all=======
dancer_pub_hql_info_all = '''
INSERT overwrite table mid_info_all partition (ds='{date}')
SELECT user_id,
       account,
       name,
       reg_time,
       act_time,
       LEVEL,
       vip,
       free_coin,
       charge_coin,
       gold,
       energy,
       cmdr_energy,
       honor,
       combat,
       guide,
       max_stage,
       item_dict,
       card_dict,
       equip_dict,
       combiner_dict,
       once_reward,
       card_assistant,
       combiner_in_use,
       card_assis_active,
       chips,
       chip_pos,
       equip_pos,
       device_mark,
       emblems,
       achievements,
       regist_ip,
       contact_skill,
       stone_dict,
       books_dict,
       reward_dict,
       acupoint_dict,
       appid,
       regist_time,
       secre_dict,
       pet_dict,
       runes,
       ride,
       horcrux
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

# ======mid_new_account=======
self_hql_new_account = '''
INSERT overwrite TABLE mid_new_account partition (ds='{date}')
select a.account, a.platform from
(SELECT
       distinct account,
       substr(account,1,instr(account,'_')-1) as platform
FROM   parse_info
WHERE ds= '{date}') a
left outer join
(
select
account
       FROM mid_info_all
       WHERE ds= '{yestoday}') b
       on a.account=b.account where b.account is null
'''

# ======武娘多语言的mid_new_account=======
dancer_mul_new_account = '''
INSERT overwrite TABLE mid_new_account partition (ds='{date}')
SELECT DISTINCT account,
                substr(account,1,instr(account,'_')-1) AS platform,
                register_lan_sort,
                appid
FROM parse_info
WHERE ds= '{date}'
  AND regexp_replace(substr(regist_time,1,10),'-','') = '{date}'
'''

mid_dic = {
    'mid_info_all': hql_info_all,
    'mid_assist': hql_assist,
    'mid_new_account': self_hql_new_account,
}

dancer_mul_mid_dic = {
    'mid_info_all': dancer_mul_hql_info_all,
    'mid_assist': hql_assist,
    'mid_new_account': dancer_mul_new_account,
}

dancer_pub_mid_dic = {
    'mid_info_all': dancer_pub_hql_info_all,
    'mid_assist': hql_assist,
    'mid_new_account': self_hql_new_account,
}

dancer_bt_mid_dic = {
    'mid_info_all': dancer_bt_hql_info_all,
    'mid_assist': hql_assist,
    'mid_new_account': self_hql_new_account,

}