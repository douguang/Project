#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Dong Junshuang
Description : 分VIP等级查看近期7日活跃玩家各资源储备情况
'''
import settings
from utils import hqls_to_dfs, update_mysql, ds_add, hql_to_df
from pandas import Series,DataFrame
import pandas as pd

if __name__ == '__main__':
    settings.set_env('superhero_vt')
    date = '20160830'

    info_sql = '''
    SELECT vip_level,
           count(uid),
           sum(chaonengzhichen),
           sum(qiangnengzhichen),
           sum(food),
           sum(metal),
           sum(gaojinengjing)
    FROM
      (SELECT ds,
              uid,
              vip_level,
              chaonengzhichen,
              qiangnengzhichen,
              food,
              metal,
              gaojinengjing,
              row_number() over(partition BY uid
                                ORDER BY ds DESC) rn
       FROM raw_info
       WHERE ds >='20160824'
         AND ds <= '20160830' )a
    WHERE rn =1
      AND vip_level>0
    GROUP BY vip_level
    ORDER BY vip_level
    '''
