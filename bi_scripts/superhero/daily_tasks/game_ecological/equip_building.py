#!/usr/bin/env python  
# encoding: utf-8  

""" 
@author: Andy 
@site:  装备打造
@software: PyCharm 
@file: equip_building.py 
@time: 17/12/19 下午3:01

装备打造 equip.equip_building
精装复炼 equip.equip_building_again
打造装备升级 equip.equip_building_up
旧装备打造 equip.equip_building_old
"""

from utils import hqls_to_dfs, ds_add, hql_to_df, date_range
import settings_dev
import pandas as pd
from pandas import DataFrame
from ipip import IP


def data_reduce(start_ds,end_ds):
    print start_ds,end_ds
    info_sql = '''
        select ds,uid,action,args,act_time from raw_action_log where ds>='{start_ds}' and ds<='{end_ds}' and action in ('equip.equip_building','equip.equip_building_old','equip.equip_building_again','equip.equip_building_up') group by ds,uid,action,args,act_time
    '''.format(start_ds=start_ds,end_ds=end_ds)
    print info_sql
    info_df = hql_to_df(info_sql)
    print info_df

    def plat_lines():
        for _, row in info_df.iterrows():
            args_type = row.args
            build_id = ''
            equip_cid = ''
            if 'build_id' in args_type:
                build_id = eval(args_type)['build_id'][0]
                print eval(args_type)['build_id'][0]
            if 'equip_cid' in args_type:
                equip_cid = eval(args_type)['equip_cid'][0]
                print eval(args_type)['equip_cid'][0]

            yield [row.ds,row.uid,row.action,build_id,equip_cid,row.act_time,]

    res_df = pd.DataFrame(plat_lines(), columns=['ds', 'uid', 'action','build_id', 'equip_cid', 'act_time',])
    print res_df.head()

    # 总打造人数 装备打造次数 精装复炼次数 打造装备升级次数 旧装备打造次数

    result_df = res_df.groupby(['ds', 'action', 'build_id', 'equip_cid']).agg({
        'uid': lambda g: g.nunique(),
        'act_time': lambda g: g.count(),
    }).reset_index().rename(columns={'uid': 'uid_num', 'act_time': 'num', })

    return res_df,result_df


if __name__ == '__main__':
    platform = 'superhero_bi'
    settings_dev.set_env(platform)
    start_date = '20171118'
    end_date = '20171218'
    res1,res2 = data_reduce(start_date, end_date,)
    res1.to_excel('/Users/kaiqigu/Documents/Superhero/%s-装备打造数据1_20171219.xlsx' % platform)
    res2.to_excel('/Users/kaiqigu/Documents/Superhero/%s-装备打造数据2_20171219.xlsx' % platform)