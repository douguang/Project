#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
@Time    : 17-7-25 上午10:26
@Author  : Andy 
@File    : omni_exchange.py
@Software: PyCharm
Description : 限时兑换  exchange.server_omni_exchange
'''

import pandas as pd

data = pd.read_excel('/home/kaiqigu/桌面/兑换活动.xls')
print data.head(3)
user_id_list,obj_list,a_typ_list,date_list,count_list =[],[],[],[],[]
for i in range(len(data)):
    try:
        date = data.iloc[i, 0]
        user_id = data.iloc[i, 1]
        a_typ = data.iloc[i, 2]
        tar = data.iloc[i, 3]
        print tar
        print date
        tar = eval(tar)
        print type(tar)
        item_list = tar

        user_id = str(user_id).split('@')[0].strip()
        print item_list
        print user_id
        obj = tar['id']
        print "__________"
        print obj


        date_list.append(date)
        user_id_list.append(user_id)
        a_typ_list.append(a_typ)
        obj_list.append(obj)
        count_list.append(1)


    except:
        pass

result_df = pd.DataFrame({'user_id': user_id_list,
                      'obj': obj_list,
                      'a_typ': a_typ_list,
                      'num': count_list,
                      'date': date_list,})

result_df.to_excel('/home/kaiqigu/桌面/机甲无双-多语言-玩家限时兑换道具_20170725.xlsx',index=False)