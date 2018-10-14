#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Dong Junshuang
Description : 越南 玩家到100级所需时间
'''
import pandas as pd
from pandas import DataFrame
import datetime

print 'please wait a mininate'
#当前数据
date = '20160504'
now_data =  pd.read_table('/Users/kaiqigu/Downloads/super_file/info_{0}'.format(date),names= ['uid','account','nick','platform_2','device','create_time','fresh_time','vip_level','level','zhandouli','food','metal','energy','nengjing','zuanshi','qiangnengzhichen','chaonengzhichen','xingdongli','xingling','jinbi','lianjingshi','shenen','gaojishenen','gaojinengjing','jingjichangdianshu'])
columns = ['uid','level','vip_level','create_time']
now_data = now_data[columns]
now_data = now_data[now_data['level']>=100]
# print now_data

#前一天数据
date = '20160503'
pre_data =  pd.read_table('/Users/kaiqigu/Downloads/super_file/total_info_{0}'.format(date),names= ['uid','account','nick','platform_2','device','create_time','fresh_time','vip_level','level','zhandouli','food','metal','energy','nengjing','zuanshi','qiangnengzhichen','chaonengzhichen','xingdongli','xingling','jinbi','lianjingshi','shenen','gaojishenen','gaojinengjing','jingjichangdianshu','ds'])
columns = ['uid','level','create_time','ds']
pre_data = pre_data[columns]
pre_data = pre_data[pre_data['level']<=100]
# print pre_data

now_data['is_new_user'] = now_data['uid'].isin(pre_data.uid.values)
new_user = now_data[now_data['is_new_user']==True]

vip_03_user =  new_user[new_user['vip_level']<=3]
vip_36_user =  new_user[new_user['vip_level']>=3]
vip_36_user =  vip_36_user[vip_36_user['vip_level']<=6]
vip_6_user =  new_user[new_user['vip_level']>6]
date = '20160504'
vip_03_user.to_excel('/Users/kaiqigu/Downloads/Excel/vip_03_user_{0}.xlsx'.format(date))
vip_36_user.to_excel('/Users/kaiqigu/Downloads/Excel/vip_36_user_{0}.xlsx'.format(date))
vip_6_user.to_excel('/Users/kaiqigu/Downloads/Excel/vip_6_user_{0}.xlsx'.format(date))
print vip_03_user
print vip_36_user
print vip_6_user
print 'Complete'
