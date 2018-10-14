#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Dong Junshuang
Description : 通过hql加工的中间数据
'''
# ======mid_info_all=======
hql_info_all = '''
INSERT overwrite table mid_info_all partition (ds='{date}')
SELECT    uid,
          account,
          device_mask,
          device_name,
          name,
          vip,
          level,
          exp,
          fame_level,
          fame,
          reg_time,
          metal,
          gas,
          anti_obj,
          metal_speed,
          gas_speed,
          anti_speed,
          alliance_id,
          alliance_name,
          domain_id,
          alliance_post,
          alliance_contribute,
          power,
          stamina,
          offline_time,
          sub_city_num,
          fort_num,
          sb_chapter_id,
          sb_pass_id,
          tiles,
          npc_wall_tiles,
          guide_nodes,
          cur_main_quest,
          time,
          log_id,
          platform,
          diamond,
          ping,
          money,
          app_id
FROM
  ( SELECT *,
           row_number() over(partition BY uid
                             ORDER BY time DESC) AS rn
   FROM
     ( SELECT *
      FROM parse_info
      WHERE ds = '{date}'
      UNION ALL SELECT *
      FROM mid_info_all
      WHERE ds = '{yestoday}' ) t1) t2
WHERE rn = 1
'''

# ======mid_info=======
hql_info = '''
INSERT overwrite TABLE mid_info partition (ds='{date}')
select uid,
          account,
          device_mask,
          device_name,
          name,
          vip,
          level,
          exp,
          fame_level,
          fame,
          reg_time,
          metal,
          gas,
          anti_obj,
          metal_speed,
          gas_speed,
          anti_speed,
          alliance_id,
          alliance_name,
          domain_id,
          alliance_post,
          alliance_contribute,
          power,
          stamina,
          offline_time,
          sub_city_num,
          fort_num,
          sb_chapter_id,
          sb_pass_id,
          tiles,
          npc_wall_tiles,
          guide_nodes,
          cur_main_quest,
          time,
          log_id,
          platform,
          diamond,
          ping,
          money,
          app_id
from
(SELECT *,
           row_number() over(partition BY uid
                             ORDER BY time desc) AS rn
from parse_info where ds='{date}') t1 where t1.rn = 1
'''


mid_dic = {
    'mid_info_all': hql_info_all,
    'mid_info': hql_info,
}
