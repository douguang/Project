#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
@Time    : 17-3-9 上午11:17
@Author  : Andy 
@File    : sanguo_weekly_report_new_vip.py
@Software: PyCharm
Description :
'''

import pandas as pd
import settings_dev
from utils import hql_to_df

slist = []
for db in ['sanguo_ks', 'sanguo_bt', 'sanguo_tl']:
    settings_dev.set_env(db)
    sql = '''
    select ds, vip, count(user_id) as num
    from mid_info_all
    where ds in ('20171101', '20171108', '20171025')
      and vip > 0
    group by ds, vip
    '''
    print sql
    used = hql_to_df(sql)
    used['youxi'] = db
    slist.append(used)
    print 'iiii'
dfs = pd.concat(slist)
print 'e'
# 本周VIP等级分组
vip_dic={1:'01-04',2:'01-04',3:'01-04',4:'01-04',5:'05-09',6:'05-09',7:'05-09',8:'05-09',9:'05-09',10:'10-12',11:'10-12',12:'10-12',13:'13-14',14:'13-14',15:'15'}
dfs['vip'] = dfs.vip.replace(vip_dic)
# ds_dic = {'20170215':'上上周','20170222':'上周','20170301':'今周',}
# dfs['ds'] = dfs.ds.replace(ds_dic)
dfs = dfs.groupby(['youxi', 'ds','vip', ]).agg({
                'num': lambda g: g.sum(),
            }).reset_index()
ll_df = dfs[dfs['ds']=='20171025']
ll_df = ll_df[['youxi', 'vip','num',]]
ll_df = ll_df.rename(columns={'num': 'num_ll',})
print ll_df.head()
l_df = dfs[dfs['ds'] == '20171101']
l_df = l_df[['youxi', 'vip','num',]]
l_df = l_df.rename(columns={'num': 'num_l',})
print l_df.head()
this_df = dfs[dfs['ds']=='20171108']
this_df = this_df[['youxi', 'vip','num',]]
print this_df.head()
res_df = pd.DataFrame(l_df).merge(this_df,on=['youxi','vip', ])
res_df = res_df.merge(ll_df,on=['youxi', 'vip', ])

res_df.to_excel('/Users/kaiqigu/Documents/Sanguo/三国周报数据/week_report_vip_group_3.xlsx', index=False)
print 'over'