#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
@Time    : 17-6-23 下午2:34
@Author  : Andy
@File    : dis_vioded_orders.py
@Software: PyCharm
Description :
'''

import settings_dev
from utils import hql_to_df, update_mysql, ds_add
import pandas as pd
from ipip import *
import datetime


def dis_vioded_orders(date):
    vioid_data_sql = '''
    SELECT t1.ds,
           t1.purchasetimemillis,
           t1.voidedtimemillis,
           t2.developerPayload,
           t2.orderid,
           t2.productid,
           t2.ip
    FROM
      ( SELECT ds,
               purchasetimemillis,
               voidedtimemillis,
               purchasetoken
       FROM parse_voided_data
       WHERE ds='{date}'
       GROUP BY ds,
                purchasetimemillis,
                voidedtimemillis,
                purchasetoken )t1
    LEFT OUTER JOIN
      ( SELECT purchasetoken,
               developerPayload,
               orderid,
               productid,
               ip
       FROM parse_sdk_nginx
       WHERE ds>='{date_in_3days}'
         AND ds<='{date}'
       GROUP BY purchasetoken,
                developerPayload,
                orderid,
                productid,
                ip )t2 ON t1.purchasetoken=t2.purchasetoken
    GROUP BY t1.ds,
             t1.purchasetimemillis,
             t1.voidedtimemillis,
             t2.developerPayload,
             t2.orderid,
             t2.productid,
             t2.ip
    '''.format(**{
        'date': date,
        'date_in_3days': ds_add(date, -3),
    })
    print vioid_data_sql
    vioid_data_df = hql_to_df(vioid_data_sql)
    print vioid_data_df.head()

    ip_df = vioid_data_df[['orderid', 'ip']]
    ip_df = ip_df[ip_df['orderid'] != None]
    ip_df['ip'] = ip_df['ip'].astype(basestring)
    IP.load(os.path.abspath("./tinyipdata_utf8.dat"))

    def ip_lines():
        for _, row in ip_df.iterrows():
            ip = row.ip
            try:
                country = IP.find(ip).strip().encode("utf8")
            except:
                country = '未知国家'
            finally:
                if '中国台湾' in country:
                    country = '台湾'
                elif '中国香港' in country:
                    country = '香港'
                elif '中国澳门' in country:
                    country = '澳门'
                elif '中国' in country:
                    country = '中国'
                yield [row.orderid, country]

    vt_df = pd.DataFrame(ip_lines(), columns=['orderid', 'country'])
    print vt_df.head()

    vioid_data_df = vioid_data_df.merge(vt_df, on='orderid', how='left')
    vioid_data_df = pd.DataFrame(vioid_data_df).dropna()

    print vioid_data_df.head()
    if vioid_data_df.__len__() != 0:
        ds_list, order_ds_list, user_id_list, order_id_list, product_id_list, voided_ds_list, ip_list, country_list = [], [], [], [], [], [], [], []
        for i in range(len(vioid_data_df)):
            ds = vioid_data_df.iloc[i, 0]
            order_ds = vioid_data_df.iloc[i, 1]
            voided_ds = vioid_data_df.iloc[i, 2]
            order_ds = datetime.datetime.fromtimestamp(int(order_ds) / 1000)
            voided_ds = datetime.datetime.fromtimestamp(int(voided_ds) / 1000)
            user_id = vioid_data_df.iloc[i, 3]
            user_id = user_id.split('-')[0].strip()
            order_id = vioid_data_df.iloc[i, 4]
            product_id = vioid_data_df.iloc[i, 5]
            ip = vioid_data_df.iloc[i, 6]
            country = vioid_data_df.iloc[i, 7]

            ds_list.append(ds)
            order_ds_list.append(order_ds)
            user_id_list.append(user_id)
            order_id_list.append(order_id)
            product_id_list.append(product_id)
            voided_ds_list.append(voided_ds)
            ip_list.append(ip)
            country_list.append(country)

        vioid_data_df = pd.DataFrame({'ds': ds_list,
                                      'order_ds': order_ds_list,
                                      'order_id': order_id_list,
                                      'product_id': product_id_list,
                                      'user_id': user_id_list,
                                      'voided_ds': voided_ds_list,
                                      'ip': ip_list,
                                      'country': country_list})
        vioid_data_df = vioid_data_df[['ds',
                                       'order_ds',
                                       'order_id',
                                       'product_id',
                                       'user_id',
                                       'voided_ds',
                                       'ip',
                                       'country', ]]
        # 更新MySQL表
        table = 'dis_voided_orders'
        print date, table
        del_sql = 'delete from {0} where ds="{1}"'.format(table, date)
        update_mysql(table, vioid_data_df, del_sql)


if __name__ == '__main__':
    settings_dev.set_env('dancer_mul')
    date = '20170630'
    dis_vioded_orders(date)
