#!/usr/bin/env python  
# encoding: utf-8  

""" 
@author: Andy 
@site:  等级礼包数据-消耗版
@software: PyCharm 
@file: dengji_gift_data.py 
@time: 17/11/16 上午9:23 
"""
import pandas as pd
import settings_dev
from utils import hql_to_df, ds_add, date_range


def data_reduce():
    settings_dev.set_env('superhero2')
    info_sql = '''
        select t3.ds,t3.reg_ds,t3.vip,t3.level,t3.diamond_num,count(distinct t3.user_id) as user_id_num from (
        select t1.ds,t1.user_id,t2.account,t2.reg_ds,t2.vip,t1.level,t1.goods_type,t1.diamond_num,t1.subtime from (
        select ds,user_id,args as level,goods_type,diamond_num,subtime from raw_spendlog where ds>='20171113' and goods_type = 'user.level_award' group by ds,user_id,level,goods_type,diamond_num,subtime
          )t1 left outer join(
            select ds,user_id,account,regexp_replace(to_date(reg_time),'-','')  as reg_ds,vip from parse_info where ds>='20171113' group by ds,user_id,account,reg_ds,vip
            )t2 on (t1.ds=t2.ds and t1.user_id=t2.user_id)
          group by t1.ds,t1.user_id,t2.account,t2.reg_ds,t2.vip,t1.level,t1.goods_type,t1.diamond_num,t1.subtime
        )t3
         group by t3.ds,t3.reg_ds,t3.vip,t3.level,t3.diamond_num
    '''
    print info_sql
    reg_df = hql_to_df(info_sql)
    print reg_df.head()

    # reg_df['level'] = reg_df['level'].astype(dict)

    def df_lines():
        for _, row in reg_df.iterrows():
            level = eval(row.level)
            level = level['lv']
            print type(row.level)
            print level
            try:
                print [row.ds, row.reg_ds, row.vip, level, row.diamond_num,row.user_id_num,]
                yield [row.ds, row.reg_ds, row.vip, level, row.diamond_num,row.user_id_num,]
            except:
                pass

    reg_df = pd.DataFrame(df_lines(), columns=['ds', 'reg_ds', 'vip', 'level', 'diamond_num', 'user_id_num',])
    reg_df = reg_df.groupby(['ds', 'reg_ds', 'vip']).agg({
        'lv': lambda g: tuple(g)
    }).reset_index()


    return reg_df


if __name__ == '__main__':
    res_df = data_reduce()

    res_df.to_excel('/Users/kaiqigu/Documents/Superhero/超级英雄2-运营-等级礼包_20171115.xlsx', index=False)
    print 'end'