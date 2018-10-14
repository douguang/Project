#!/usr/bin/env python  
# encoding: utf-8  

""" 
@author: Andy 
@site:  分服每日充值
@software: PyCharm 
@file: server_order_money.py 
@time: 17/9/8 下午5:34 
"""

from utils import date_range, ds_add, hql_to_df
import settings_dev
import pandas as pd


def tw_ltv():
    equipment_sql = '''
select t1.ds,t1.server_id,t1.dau,t1.act_ds,t2.min_ds,t2.reg_num,datediff(to_date(t1.act_ds),to_date(t2.min_ds))+1 as dd,t3.order_money from (
select regexp_replace(substr(act_time,1,10),'-','') as ds,reverse(substring(reverse(user_id),8)) as server_id,count(distinct user_id) as dau,to_date(act_time) as act_ds from raw_info where ds>='20160419' group by ds,server_id,act_ds
)t1 left outer join(
select reverse(substring(reverse(user_id),8)) as server_id,count(distinct user_id) as reg_num,min(to_date(reg_time)) as min_ds from mid_info_all where ds='20170801' and reg_time not like '%1970%' group by server_id
)t2 on t1.server_id=t2.server_id left outer join(
select ds,reverse(substring(reverse(user_id),8)) as server_id,sum(order_money) as order_money from raw_paylog where ds>='20160419' and platform_2 != 'admin_test' and platform_2 != 'admin' group by ds,server_id
)t3 on (t1.ds=t3.ds and t1.server_id=t3.server_id)
group by t1.ds,t1.server_id,t1.dau,t1.act_ds,t2.min_ds,t2.reg_num,dd,order_money
'''
    equip_df = hql_to_df(equipment_sql).fillna(0)
    print equip_df.head(3)

    return equip_df


if __name__ == '__main__':
    for platform in ['sanguo_ks',]:
        settings_dev.set_env(platform)
        res = tw_ltv()
        res.to_excel(r'/home/kaiqigu/桌面/机甲无双-金山-分服每日充值_20170802.xlsx', index=False)
    print "end"