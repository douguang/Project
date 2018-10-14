#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Dong Junshuang
Description : 属性改造情况
'''
import settings
from utils import hqls_to_dfs, update_mysql, ds_add, hql_to_df
from pandas import Series,DataFrame
import pandas as pd

if __name__ == '__main__':
    settings.set_env('superhero_vt')
    end_date = '20161009'
    attr_sql = '''
    SELECT a.card_id,
           wugong,
           mogong,
           shengming,
           sudu
    FROM
      (SELECT ds,
              uid,
              card_id,
              wugong,
              mogong,
              shengming,
              sudu
       FROM raw_card
       WHERE ds >='{start_date}'
         AND ds <= '{end_date}'
         AND is_fight = 1) a join
      (SELECT ds,uid
       FROM
         (SELECT ds,uid,vip_level,row_number() over(partition BY uid
                                                    ORDER BY ds DESC) rn
          FROM raw_info
          WHERE ds >='{start_date}'
            AND ds <= '{end_date}' )b
       WHERE rn =1
         AND vip_level>=3)c ON a.ds = c.ds
    AND a.uid = c.uid
'''.format(start_date=ds_add(end_date,-6),end_date=end_date)
    # attr_sql = '''
    # SELECT a.card_id card_id,
    #        wugong,
    #        mogong,
    #        shengming,
    #        sudu
    # FROM
    #   ( SELECT ds,
    #            uid,
    #            card_id,
    #            wugong,
    #            mogong,
    #            shengming,
    #            sudu
    #    FROM raw_card
    #    WHERE ds >='20160824'
    #      AND ds <= '20160830'
    #      and is_fight = 1) a join
    #   ( SELECT max(ds) ds,uid,card_id
    #    FROM raw_card
    #    WHERE ds >='20160824'
    #      AND ds <= '20160830'
    #    GROUP BY uid,card_id )b ON a.ds = b.ds
    # AND a.uid =b.uid
    # AND a.card_id = b.card_id
    # JOIN
    #   ( SELECT uid
    #    FROM mid_info_all
    #    WHERE ds ='20160830'
    #      AND vip_level>=3 )c ON a.uid = c.uid
    # AND b.uid = c.uid
    # '''
    attr_df = hql_to_df(attr_sql)

    wugong_df = (attr_df
        .groupby('wugong')
        .count()
        .card_id
        .reset_index()
        .rename(columns={'card_id':'wugong_num','wugong':'attr'}))

    mogong_df = (attr_df
        .groupby('mogong')
        .count()
        .card_id
        .reset_index()
        .rename(columns={'card_id':'mogong_num','mogong':'attr'}))

    shengming_df = (attr_df
        .groupby('shengming')
        .count()
        .card_id
        .reset_index()
        .rename(columns={'card_id':'shengming_num','shengming':'attr'}))

    sudu_df = (attr_df
        .groupby('sudu')
        .count()
        .card_id
        .reset_index()
        .rename(columns={'card_id':'sudu_num','sudu':'attr'}))

    result_df = (wugong_df.merge(mogong_df,on='attr',how='outer')
        .merge(shengming_df,on='attr',how='outer')
        .merge(sudu_df,on='attr',how='outer')
        )

    result_df.to_excel('/Users/kaiqigu/Downloads/Excel/attr.xlsx')






