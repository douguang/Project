#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Dong Junshuang
Description : 橙色英魂数据
橙色英魂：末尾为5
红色英魂：末尾为6（20160913日新增的英魂）
金色英魂：末尾为7
注：当前未排除测试用户
'''
import settings_dev
from utils import ds_add
from utils import hql_to_df

if __name__ == '__main__':
    settings_dev.set_env('superhero_bi')
    end_date = '20170604'
    sql = '''
    SELECT a.uid,
           b.vip_level,
           a.soul_id
    FROM
      (SELECT ds,
              uid,
              soul_id
       FROM raw_soul
       WHERE ds = '{end_date}'
         AND substr(soul_id,3,1) = '7'
         AND is_fight <> -1)a
    JOIN
      (SELECT uid,
              max(ds) ds,
              max(vip_level) vip_level
       FROM raw_info
       WHERE ds >= '{start_date}'
         AND ds <= '{end_date}'
         and substr(uid,1,1) = 'g'
       GROUP BY uid)b ON a.ds = b.ds
    AND a.uid = b.uid
    '''.format(end_date=end_date,
               start_date=ds_add(end_date, -6))
    df = hql_to_df(sql)

    soul_df = (df.groupby(['uid', 'vip_level']).count().soul_id.reset_index()
               .rename(columns={'soul_id': 'soul_num'}))

    result = (soul_df.groupby(['soul_num', 'vip_level']).count().reset_index()
              .rename(columns={'uid': 'uid_num'}))

    result_df = (result.pivot_table('uid_num', ['soul_num'], 'vip_level')
                 .reset_index().fillna(0))

    result_df.to_excel('/Users/kaiqigu/Documents/Excel/jin_soul.xlsx')
