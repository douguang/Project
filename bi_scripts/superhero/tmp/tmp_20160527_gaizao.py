#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Dong Junshuang
Description : 越南 属性改造情况
'''
import settings
from utils import hqls_to_dfs, update_mysql, ds_add
from pandas import Series,DataFrame
from utils import get_config
import pandas as pd


if __name__ == '__main__':
    settings.set_env('superhero_vt')
    print 'please wait a minuate'
    sql = '''
    select uid,wugong,mogong,shengming,sudu
    from
    (
    select uid,is_fight,wugong,mogong,shengming,sudu,row_number() over(partition by uid order by ds desc) as rn
    from raw_card where ds >= '20160520' and ds <= '20160526'
    ) a
    where is_fight =1
    and rn=1
    '''
    uid_sql = '''
    select uid,vip_level from
    (select uid,vip_level,row_number() over(partition by uid order by ds desc) as rn
    from raw_info
    where ds >= '20160520' and ds <= '20160526'
    and vip_level>=6
    ) a
    where rn = 1
    '''
    gaizao_df,uid_df = hqls_to_dfs([sql,uid_sql])
    gaizao_df['is_vip6_uid'] = gaizao_df['uid'].isin(uid_df.uid.values)
    gaizao_df = gaizao_df[gaizao_df['is_vip6_uid']]
    wugong_df = gaizao_df.groupby('wugong').count().reset_index()
    mogong_df = gaizao_df.groupby('mogong').count().reset_index()
    shengming_df = gaizao_df.groupby('shengming').count().reset_index()
    sudu_df = gaizao_df.groupby('sudu').count().reset_index()
    wugong_df['level'] = wugong_df['wugong']
    wugong_df['wugong_uid'] = wugong_df['uid']
    columns = ['level','wugong_uid']
    wugong_df = wugong_df[columns]

    mogong_df['level'] = mogong_df['mogong']
    mogong_df['mogong_uid'] = mogong_df['uid']
    columns = ['level','mogong_uid']
    mogong_df = mogong_df[columns]

    shengming_df['level'] = shengming_df['shengming']
    shengming_df['wshengming_uid'] = shengming_df['uid']
    columns = ['level','wshengming_uid']
    shengming_df = shengming_df[columns]

    sudu_df['level'] = sudu_df['sudu']
    sudu_df['sudu_uid'] = sudu_df['uid']
    columns = ['level','sudu_uid']
    sudu_df = sudu_df[columns]

    result_data = (wugong_df
                            .merge(mogong_df,on=['level'],how='outer')
                            .merge(shengming_df,on=['level'],how='outer')
                            .merge(sudu_df,on=['level'],how='outer')
                    )
    result_data = result_data.fillna(0)

    result_data.to_excel('/Users/kaiqigu/Downloads/Excel/gaizao.xlsx')

