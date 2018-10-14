#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
@Time    : 17-7-21 下午5:17
@Author  : Andy 
@File    : items.py
@Software: PyCharm
Description :
'''

#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
@Time    : 17-6-5 下午4:05
@Author  : Andy
@File    : card.py
@Software: PyCharm
Description :   select ds,body.a_usr,body.a_typ, body.a_tar,body.a_rst  from raw_actionlog where ds='20170603'  and account= 'kaiqigu_10492876'

select ds,body.a_usr,body.a_typ, body.a_tar,body.a_rst  from raw_actionlog where ds='20171006' and body.a_rst  != '' and log_t>= '1507219200' and log_t<= '1507257660'
'''
import settings_dev
import pandas as pd
from utils import hql_to_df, date_range, ds_add

settings_dev.set_env('sanguo_tl')
data_sql = '''
    select ds,body.a_usr,body.a_typ, body.a_tar,body.a_rst,log_t  from raw_actionlog where ds>='20171029' and ds<='20171101' and body.a_usr like 'th1299126973%' 
'''
print data_sql
data = hql_to_df(data_sql,'hive')
print data.head()

user_id_list,obj_list,a_typ_list,date_list,diff_list,after_list,before_list,log_t_list=[],[],[],[],[],[],[],[]
for i in range(len(data)):
    try:
        date = data.iloc[i, 0]
        user_id = data.iloc[i, 1]
        a_typ = data.iloc[i, 2]
        tar = data.iloc[i, 4]
        log_t = data.iloc[i, 5]
        # print tar
        # print date
        tar = eval(tar)
        # print type(tar)
        item_list = tar

        user_id = str(user_id).split('@')[0].strip()
        # print item_list
        # print user_id
        for a in item_list:
            obj = a['obj']
            if obj =='Item@5' in obj:
            # if 'Card@7016' in obj:
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

result_df.to_excel('/Users/kaiqigu/Documents/Sanguo/机甲无双-多语言版-th1299126973的体力药水变化数据_20171102.xlsx',index=False)


# pay_sql = '''
# select user_id,sum(order_money) as order_money from raw_paylog where ds>='20170701' and platform_2 != 'admin_test' group by user_id
# '''
# print pay_sql
# pay_df = hql_to_df(pay_sql)
# print pay_df.head()
#
# res_df = result_df.groupby(['date','user_id','obj']).agg(
#         {'diff': lambda g: g.count()}).reset_index()
#
# res_df = res_df.merge(pay_df, on=['user_id',], how='left')
# res_df = pd.DataFrame(res_df).fillna(0)
# result_df.to_excel('/Users/kaiqigu/Documents/Sanguo/机甲无双-台湾版-tw274201805卡牌变化数据_20171011-1.xlsx',index=False)