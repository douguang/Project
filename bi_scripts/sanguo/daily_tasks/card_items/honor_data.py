#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
@Time    : 17-7-26 上午9:31
@Author  : Andy 
@File    : honor_data.py
@Software: PyCharm
Description :
'''


import pandas as pd
import settings_dev
from utils import hql_to_df, date_range, ds_add


settings_dev.set_env('sanguo_tl')
data_sql = '''
    select ds,body.a_usr,body.a_typ, body.a_tar,body.a_rst,log_t  from raw_actionlog where ds>='20171001'  and body.a_usr= 'th1052947845@th105'
    '''
print data_sql
data = hql_to_df(data_sql,'hive')
print data.head()

user_id_list,obj_list,a_typ_list,date_list,diff_list,after_list,before_list,log_t_list=[],[],[],[],[],[],[],[]
for i in range(len(data)):
    try:

        tar = data.iloc[i, 4]
        date = data.iloc[i, 0]
        if 'honor' in tar:
            print tar
            print date
            tar = eval(tar)
            user_id = data.iloc[i, 1]
            a_typ = data.iloc[i, 2]
            log_t = data.iloc[i, 5]
            user_id = str(user_id).split('@')[0].strip()
            print user_id
            for a in tar:
                obj = a['obj']
                if obj == 'honor' or obj == 'max_honor':
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

result_df.to_excel('/Users/kaiqigu/Documents/Sanguo/机甲无双-多语言-th1052947845玩家功勋变化_20171011-2.xlsx',index=False)