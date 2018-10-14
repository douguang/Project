#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
@Time    : 17-8-2 上午11:52
@Author  : Andy 
@File    : user_id_ip.py
@Software: PyCharm
Description :
'''


from utils import date_range, ds_add, hql_to_df
import settings_dev
import pandas as pd


def tw_ltv(date):
    equipment_sql = '''
        select t1.account,t1.user_id,t1.platform,t1.act_time,t2.order_money,t3.ts,t4.ip  from (
          select account,user_id,platform,act_time from mid_info_all where ds='20170801' and regexp_replace(substring(reg_time,1,10),'-','') = '{date}' group by  account,user_id,platform,act_time
        )t1 left outer join(
          select user_id,sum(order_money) as order_money  from raw_paylog where ds>='{date}' and platform_2 != 'admin_test' and platform_2 != 'admin'  group by user_id
        )t2 on t1.user_id=t2.user_id left outer join(
          select user_token as user_id,min(ts)  as ts from parse_nginx where ds='{date}' and user_token != '' and ts != ''  group by user_id
        )t3 on t1.user_id=t3.user_id left outer join(
          select ip,user_token as user_id,ts from parse_nginx where ds='{date}' and user_token != '' and ts != '' group by ip,user_id,ts
        )t4 on (t3.user_id=t4.user_id and t3.ts=t4.ts)
        group by t1.account,t1.user_id,t1.platform,t1.act_time,t2.order_money,t3.ts,t4.ip
    '''.format(**{'date':date,})
    equip_df = hql_to_df(equipment_sql).fillna(0)
    print equip_df.head(3)

    return equip_df


if __name__ == '__main__':
    res_list = []
    for platform in ['sanguo_tl',]:
        settings_dev.set_env(platform)
        for date in date_range('20170103','20170801'):
            print date
            res = tw_ltv(date)
            res_list.append(res)
    pd.concat(res_list).to_excel(r'/home/kaiqigu/桌面/机甲无双-多语言-玩家IP_20170802.xlsx', index=False)
    print "end"

