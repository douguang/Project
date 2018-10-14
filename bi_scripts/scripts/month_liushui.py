#!/usr/bin/env python  
# encoding: utf-8  

""" 
@author: Andy 
@site:  月流水
@software: PyCharm 
@file: month_liushui.py 
@time: 18/4/2 上午11:15 
"""

import calendar
import settings_dev
from utils import ds_add, hql_to_df, update_mysql, hqls_to_dfs, date_range


def data_reduce(start_ds,end_ds,platform):
    print platform
    settings_dev.set_env(platform)
    if platform in ['sanguo_tx','sanguo_tw','sanguo_tt','sanguo_tl','sanguo_guandu','sanguo_mth','sanguo_ks','sanguo_kr','sanguo_ios','sanguo_bt','dancer_tx','dancer_tw','dancer_pub','dancer_mul','dancer_kr','dancer_cgame','dancer_bt','superhero_bi','superhero_vt','superhero_mul','superhero_qiku','metal_pub']:
        info_sql = '''
        select platform_2,sum(order_money) as order_money from raw_paylog where ds>='{start_ds}' and ds<='{end_ds}' and platform_2 <> 'admin' and platform_2 <> 'admin_test' group by platform_2
        '''.format(**{'start_ds': start_ds, 'end_ds': end_ds})
        print info_sql
        info_df = hql_to_df(info_sql)
        print info_df.head()
        info_df.to_excel(r'/Users/kaiqigu/Desktop/month_liushui/sanguo/%s_%s-%s-月流水.xlsx' % (platform, start_ds, end_ds),index=False)

    if platform in ['dancer_pub',]:
        info_sql = '''
          select order_id,order_money, order_rmb, currency_type, order_time, platform_2, product_id, user_id, appid from raw_paylog where ds>='{start_ds}' and ds<='{end_ds}' and substr(order_id, 1, 3) in ('ali', 'wei', 'uni') 
        '''.format(**{'start_ds': start_ds, 'end_ds': end_ds})
        print info_sql
        info_df = hql_to_df(info_sql)
        print info_df.head()
        info_df.to_excel(r'/Users/kaiqigu/Desktop/month_liushui/sanguo/%s_%s-%s-官网包-微信_支付宝_银联-月流水.xlsx' % (platform, start_ds, end_ds),index=False)

    if platform in ['sanguo_ks',]:
        info_sql = '''
          select order_id,order_money,order_time, platform_2, product_id, user_id from raw_paylog where ds>='{start_ds}' and ds<='{end_ds}' and substr(order_id, 1, 3) in ('ali', 'wei', 'uni')  
        '''.format(**{'start_ds': start_ds, 'end_ds': end_ds})
        print info_sql
        info_df = hql_to_df(info_sql)
        print info_df.head()
        info_df.to_excel(r'/Users/kaiqigu/Desktop/month_liushui/sanguo/%s_%s-%s-官网包-微信_支付宝_银联-月流水.xlsx' % (platform, start_ds, end_ds),index=False)

    if platform in ['dancer_pub', ]:
        info_sql = '''
        select platform_2,appid,sum(order_money) as order_money from raw_paylog where ds>='{start_ds}' and ds<='{end_ds}' group by  platform_2,appid
        '''.format(**{'start_ds': start_ds, 'end_ds': end_ds})
        print info_sql
        info_df = hql_to_df(info_sql)
        print info_df.head()
        info_df.to_excel(r'/Users/kaiqigu/Desktop/month_liushui/sanguo/%s_%s-%s-分渠道分APPID-月流水.xlsx' % (platform, start_ds, end_ds),index=False)

    if platform in ['dancer_pub', ]:
        info_sql =     '''
            select t1.user_id, appid, t2.platform_2, t2.pay from 
              (select user_id, appid from mid_info_all where ds='{end_ds}' and account like '%ios%' and regexp_replace(to_date(act_time),'-','')>='{start_ds}') t1
            inner join
              (select user_id, sum(order_money) as pay, platform_2 from raw_paylog where ds>='{start_ds}' and ds<='{end_ds}' and platform_2<>'admin_test'  and platform_2<>'admin' group by user_id, platform_2) t2
            on t1.user_id=t2.user_id
        '''.format(**{'start_ds': start_ds, 'end_ds': end_ds})
        print info_sql
        info_df = hql_to_df(info_sql)
        print info_df.head()
        info_df.to_excel(r'/Users/kaiqigu/Desktop/month_liushui/sanguo/%s_%s-%s-IOS包-月流水.xlsx' % (platform, start_ds, end_ds),index=False)

    if platform in ['jianniang_pub','jianniang_tw','jianniang_bt']:
        info_sql = '''
        select sum(order_rmb), platform from raw_paylog where ds>='{start_ds}' and ds<='{end_ds}' and status=1 and admin=0 group by platform
        '''.format(**{'start_ds': start_ds, 'end_ds': end_ds})
        print info_sql
        info_df = hql_to_df(info_sql)
        print info_df.head()
        info_df.to_excel(r'/Users/kaiqigu/Desktop/month_liushui/sanguo/%s_%s-%s-月流水.xlsx' % (platform, start_ds, end_ds),index=False)


if __name__ == '__main__':
    for platform in ['sanguo_tx','sanguo_tw','sanguo_tt','sanguo_tl','sanguo_ks','sanguo_guandu','metal_pub','sanguo_kr','sanguo_ios','sanguo_bt','dancer_tx','dancer_tw','dancer_pub','dancer_mul','dancer_kr','dancer_cgame','dancer_bt','jianniang_bt','jianniang_pub','jianniang_tw','superhero_bi','superhero_vt','superhero_mul','superhero_qiku']:
        start_ds='20180501'
        end_ds = '20180531'
        data_reduce(start_ds,end_ds,platform)
    print "end"