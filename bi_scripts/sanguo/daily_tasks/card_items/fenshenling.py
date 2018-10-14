#!/usr/bin/env python  
# encoding: utf-8  

""" 
@author: Andy 
@site:  
@software: PyCharm 
@file: fenshenling.py 
@time: 17/9/1 下午7:15 
"""


import settings_dev
from utils import hql_to_df, format_date, ds_add
import pandas as pd

settings_dev.set_env('sanguo_bt')

sql = '''
select user_id,item_dict from mid_info_all where ds='20171006' and act_time >='2017-10-06 00:00:00' and act_time <='2017-10-06 10:41:00'  group by user_id,item_dict
'''

data = hql_to_df(sql)
print data.head(3)

user_id_list,obj_list,num_list=[],[],[]
for i in range(len(data)):
    try:
        print '********'
        user_id = data.iloc[i, 0]
        obj = data.iloc[i, 1]
        obj = eval(obj)
        print user_id
        print obj
        print type(obj)
        print obj.get(30,0)
        num = obj.get(30,0)[0]
        print num

        user_id_list.append(user_id)
        obj_list.append('30')
        num_list.append(num)

        num_33 = obj.get(33, 0)[0]
        user_id_list.append(user_id)
        obj_list.append('33')
        num_list.append(num_33)

    except:
        pass

result_df = pd.DataFrame({'user_id': user_id_list,
                      'obj': obj_list,
                      'num': num_list})

result_df.to_excel('/Users/kaiqigu/Documents/Sanguo/机甲无双-金山-玩家的分身令道具数量_20170913.xlsx',index=False)