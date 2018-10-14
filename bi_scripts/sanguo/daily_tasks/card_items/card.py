#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
@Time    : 17-6-5 下午4:05
@Author  : Andy 
@File    : card.py
@Software: PyCharm
Description :   select ds,body.a_usr,body.a_typ, body.a_tar,body.a_rst  from raw_actionlog where ds='20170603'  and account= 'kaiqigu_10492876'
'''

import pandas as pd

data = pd.read_excel('/home/kaiqigu/桌面/机甲无双-多语言-玩家功勋查询15-17_20170726.xls')
print data.head(3)
user_id_list,obj_list,a_typ_list,date_list,diff_list,after_list,before_list,log_t_list=[],[],[],[],[],[],[],[]
for i in range(len(data)):
    try:
        date = data.iloc[i, 0]
        user_id = data.iloc[i, 1]
        a_typ = data.iloc[i, 2]
        tar = data.iloc[i, 4]
        log_t = data.iloc[i, 5]
        if 'Item@3601' in tar:
            print tar
            print date
            tar = eval(tar)
            print type(tar)
            item_list = tar

            user_id = str(user_id).split('@')[0].strip()
            print item_list
            print user_id
            for a in item_list:
                obj = a['obj']

                if obj.find("Item@3601") != -1:
                    print "__________"
                    print obj
                    diff = a['diff']
                    after = a['after']
                    before = a['before']

                    date_list.append(date)
                    user_id_list.append(user_id)
                    a_typ_list.append(a_typ)
                    obj_list.append(obj)
                    diff_list.append(diff)
                    after_list.append(after)
                    before_list.append(before)
                    log_t_list.append(log_t)

    except:
        pass

result_df = pd.DataFrame({'user_id': user_id_list,
                      'obj': obj_list,
                      'a_typ': a_typ_list,
                      'date': date_list,
                      'diff': diff_list,
                      'after': after_list,
                      'before': before_list,
                      'log_t': log_t_list})

result_df.to_excel('/home/kaiqigu/桌面/机甲无双-多语言-15-17-玩家的卡牌Item@3601碎片变化.xlsx',index=False)