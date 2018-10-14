#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
@Time    : 17-7-25 下午6:39
@Author  : Andy 
@File    : pay_log_country.py
@Software: PyCharm
Description :   苹果
'''


from utils import date_range, ds_add, hql_to_df
import settings_dev
import pandas as pd
from ipip import *

def tw_ltv(start_ds, end_ds):
    dates = date_range(start_ds, end_ds)
    ltvday = 15
    ltv_days = range(1, ltvday + 1)
    # ltv_days = [1,2,3,4,5,6]
    # 所有的充值数据
    pay_sql = '''
    select ds,order_id,admin,gift_coin,level,old_coin,order_coin,order_money,order_time,platform_2,product_id,raw_data,reason,scheme_id,user_id,currency_num,currency,pay_tp,language,address as ip,equip
    from raw_paylog
    where ds >= '{start_ds}'
      and ds <= '{end_ds}'
      and platform_2 != 'admin_test'
      group by ds,order_id,admin,gift_coin,level,old_coin,order_coin,order_money,order_time,platform_2,product_id,raw_data,reason,scheme_id,user_id,currency_num,currency,pay_tp,language,address,equip
    '''.format(start_ds=start_ds, end_ds=end_ds)
    print pay_sql
    ip_df = hql_to_df(pay_sql).rename(columns={'address':'ip',})
    ip_df = pd.DataFrame(ip_df).fillna(0)
    print ip_df.head()

    # 国际版分国家
    ip_mid_sql = '''
             select user_id,ip from user_identifier_info  where ds='{end_ds}' group by user_id,ip
        '''.format(start_ds=start_ds, end_ds=end_ds)
    print ip_mid_sql
    ip_mid_df = hql_to_df(ip_mid_sql)
    print ip_mid_df
    ip_mid_df = ip_mid_df.sort_values(by=['user_id', 'ip', ], ascending=False)
    ip_mid_df = ip_mid_df.drop_duplicates(subset=['user_id', ], keep='first')

    ip_df['ip'] = ip_df['ip'].astype(basestring)
    IP.load(os.path.abspath("./tinyipdata_utf8.dat"))

    def ip_lines():
        for _, row in ip_df.iterrows():
            # print row
            ip = row.ip
            try:
                if ip == '' or ip=='0.0.0.0':
                    # print '*************888'
                    mid_ip = ip_mid_df[ip_mid_df['user_id']==row.user_id]
                    # print mid_ip
                    # print type(mid_ip)
                    # print mid_ip['ip']
                    ip = mid_ip['ip'].item()
                    print 'ip:',ip
                    country = IP.find(ip).strip().encode("utf8")
                    if '中国台湾' in country:
                        country = '台湾'
                    elif '中国香港' in country:
                        country = '香港'
                    elif '中国澳门' in country:
                        country = '澳门'
                    elif '中国' in country:
                        country = '中国'
                    print country

                else:
                    country = IP.find(ip).strip().encode("utf8")
                    if '中国台湾' in country:
                        country = '台湾'
                    elif '中国香港' in country:
                        country = '香港'
                    elif '中国澳门' in country:
                        country = '澳门'
                    elif '中国' in country:
                        country = '中国'
                    print country
                print [row.ds, row.order_id, row.admin, row.gift_coin, row.level, row.old_coin, row.order_coin, row.order_money, row.order_time, row.platform_2, row.product_id, row.raw_data, row.reason, row.scheme_id, row.user_id, row.currency_num, row.currency, row.pay_tp, row.language, ip,country,row.equip,]
                yield [row.ds, row.order_id, row.admin, row.gift_coin, row.level, row.old_coin, row.order_coin, row.order_money, row.order_time, row.platform_2, row.product_id, row.raw_data, row.reason, row.scheme_id, row.user_id, row.currency_num, row.currency, row.pay_tp, row.language, ip,country,row.equip,]
            except:
                print '================'
                print row
                country = ''
                ip = row.ip
                yield [row.ds, row.order_id, row.admin, row.gift_coin, row.level, row.old_coin, row.order_coin, row.order_money, row.order_time, row.platform_2, row.product_id, row.raw_data, row.reason, row.scheme_id, row.user_id, row.currency_num, row.currency, row.pay_tp, row.language, ip,country,row.equip,]

                pass

    vt_df = pd.DataFrame(ip_lines(),columns=['ds','order_id','admin','gift_coin','level','old_coin','order_coin','order_money','order_time','platform_2','product_id','raw_data','reason','scheme_id','user_id','currency_num','currency','pay_tp','language','ip','country','equip'])
    # vt_df = pd.DataFrame(ip_lines(), columns=['user_id', 'country'])
    print vt_df.head()

    vt_df.to_excel(r'E:\sanguo_tl-paylog-appid-0901-1127_20171128-2.xlsx', index=False)


if __name__ == '__main__':
    for platform in ['sanguo_tl']:
        settings_dev.set_env(platform)
        tw_ltv('20170901', '20171127',)
