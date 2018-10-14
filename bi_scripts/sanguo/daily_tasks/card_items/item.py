#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
@Time    : 17-6-5 下午4:05
@Author  : Andy 
@File    : item.py
@Software: PyCharm
Description :
'''

import pandas as pd

data = pd.read_excel('/home/kaiqigu/桌面/机甲无双-泰国-玩家卡牌获取.xls')
print data.head(3)
user_id_list,obj_list,a_typ_list,date_list,diff_list,after_list,before_list=[],[],[],[],[],[],[]
for i in range(len(data)):
    try:

        tar = data.iloc[i, 2]
        date = data.iloc[i, 10]
        if 'Item@1001' in tar:
            print tar
            print date
            tar = eval(tar)
            print type(tar)
            item_list = tar['a_rst']
            user_id = tar['a_usr']
            a_typ = tar['a_typ']
            user_id = str(user_id).split('@')[0].strip()
            print item_list
            print user_id
            for a in item_list:
                obj = a['obj']
                if obj == 'Item@1003':
                    diff = a['diff']
                    after = a['after']
                    before = a['before']

                    user_id_list.append(user_id)
                    obj_list.append(obj)
                    a_typ_list.append(a_typ)
                    date_list.append(date)
                    diff_list.append(diff)
                    after_list.append(after)
                    before_list.append(before)

    except:
        pass

result_df = pd.DataFrame({'user_id': user_id_list,
                      'obj': obj_list,
                      'a_typ': a_typ_list,
                      'date': date_list,
                      'diff': diff_list,
                      'after': after_list,
                      'before': before_list})

result_df.to_excel('/home/kaiqigu/桌面/机甲无双-泰国-玩家的1003碎片变化.xls',index=False)