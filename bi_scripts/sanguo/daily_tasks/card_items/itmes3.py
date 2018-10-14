#!/usr/bin/env python  
# encoding: utf-8  

""" 
@author: Andy 
@site:  
@software: PyCharm 
@file: itmes3.py 
@time: 17/10/7 上午12:55
Description :   select ds,body.a_usr,body.a_typ, body.a_tar,body.a_rst  from raw_actionlog where ds='20170603'  and account= 'kaiqigu_10492876'

select ds,body.a_usr,body.a_typ, body.a_tar,body.a_rst  from raw_actionlog where ds='20171006' and body.a_rst  != '' and log_t>= '1507219200' and log_t<= '1507257660'
"""

import settings_dev
import pandas as pd
from utils import hql_to_df, date_range, ds_add

settings_dev.set_env('sanguo_bt')
data_sql = '''
    select ds,body.a_usr,body.a_typ, body.a_tar,body.a_rst  from raw_actionlog where ds>='20171006'  and ds<='20171007' and body.a_typ like '%omni_exchange%' and log_t>= '1507219200' and log_t< '1507392000'
'''
print data_sql
data = hql_to_df(data_sql,'hive')
print data.head()

user_id_list,obj_list,a_typ_list,date_list=[],[],[],[]
for i in range(len(data)):
    try:
        date = data.iloc[i, 0]
        user_id = data.iloc[i, 1]
        a_typ = data.iloc[i, 2]
        tar = data.iloc[i, 3]
        # print tar
        # print date
        if 'id' in tar:
            tar = eval(tar)
            # print type(tar)
            obj = tar['id']

            user_id = str(user_id).split('@')[0].strip()
            # print item_list
            # print user_id

            date_list.append(date)
            user_id_list.append(user_id)
            a_typ_list.append(a_typ)
            obj_list.append(obj)


    except:
        pass

result_df = pd.DataFrame({'user_id': user_id_list,
                      'obj': obj_list,
                      'a_typ': a_typ_list,
                      'ds': date_list,})

# result_df.to_excel('/Users/kaiqigu/Documents/Sanguo/机甲无双-变态版-bug数据_20171009.xlsx',index=False)


pay_sql = '''
select user_id,sum(order_money) as order_money from raw_paylog where ds>='20170701' and platform_2 != 'admin_test' group by user_id
'''
print pay_sql
pay_df = hql_to_df(pay_sql)
print pay_df.head()

res_df = result_df.groupby(['ds', 'user_id', 'obj',]).a_typ.count().reset_index().rename(
        columns={'a_typ': 'num'})
print '---'
print res_df.head()

res_df = res_df.merge(pay_df, on=['user_id',], how='left').fillna(0)

coin_spend_sql = '''
select ds,user_id,sum(coin_num) as coin_spend from raw_spendlog where ds>='20171006' and ds<='20171007' and goods_type like '%group_buy.group_active_buy%' group by ds,user_id
'''
print coin_spend_sql
coin_spend_df = hql_to_df(coin_spend_sql)
print coin_spend_df.head()
res_df = res_df.merge(coin_spend_df, on=['ds','user_id',], how='left').fillna(0)
res_df.to_excel('/Users/kaiqigu/Documents/Sanguo/机甲无双-变态版-bug数据_20171010.xlsx',index=False)
