#!/usr/bin/env python  
# encoding: utf-8  

""" 
@author: Andy 
@site:  新手引导
@software: PyCharm 
@file: new_user_guide.py 
@time: 18/1/30 下午2:29 
"""


from ipip import *
import settings_dev
import pandas as pd
from utils import hql_to_df
from utils import ds_add
from utils import get_server_days,date_range

def data_reduce():
    info_sql = '''
        select t1.*,t2.reg_ds from (
        select user_id,log_t,a_typ,a_tar,row_number() over(partition by user_id order by log_t desc) as rn from parse_actionlog 
        where ds='20180129' and a_typ='user.guide' 
        group by user_id,log_t,a_typ,a_tar
        )t1 
        left outer join(
          select user_id,to_date(reg_time)as reg_ds from mid_info_all where ds='20180129' group by user_id,reg_ds
          )t2 on t1.user_id=t2.user_id
        where t1.rn =1
    '''
    print info_sql
    info_df = hql_to_df(info_sql).fillna(0.0)
    # info_df = info_df[info_df['ds']>'20170820']
    print info_df.head()

    def ip_lines():
        for _, row in info_df.iterrows():
            user_id = row.user_id
            reg_ds = row.reg_ds
            a_tar = row.a_tar
            guide_team = ''
            guide_id = ''
            identifier = ''
            devicename = ''
            try:
                a_tar = eval(a_tar)
                guide_id = eval(a_tar['guide_team'])
                guide_team = eval(a_tar['guide_id'])
                identifier = eval(a_tar['identifier'])
                devicename = eval(a_tar['devicename'])
            except:
                pass
            yield [user_id,reg_ds, guide_team,guide_id,identifier,devicename]


    result = pd.DataFrame(ip_lines(), columns=['user_id', 'reg_ds', 'guide_team', 'guide_id','identifier', 'devicename',])
    print result.head()
    result.to_excel(r'/Users/kaiqigu/Documents/Sanguo/合金装甲-测试版-新手引导-1_20180130.xlsx', index=False)

    result_df = result.groupby(['reg_ds','guide_team','guide_id']).agg({
        'user_id': lambda g: g.nunique(),
    }).reset_index().rename(
        columns={'user_id': 'user_id_num',})
    result_df.to_excel(r'/Users/kaiqigu/Documents/Sanguo/合金装甲-测试版-新手引导-2_20180130.xlsx', index=False)

if __name__ == '__main__':
    settings_dev.set_env('metal_beta')
    a = data_reduce()
    print "end"