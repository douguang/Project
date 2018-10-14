#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
@Time    : 2017/9/26 0026 14:53
@Author  : Andy 
@File    : tmp_demo.py
@Software: PyCharm
Description :
'''



from ipip import *
from utils import date_range, ds_add, hql_to_df
import settings_dev
import pandas as pd


def tw_ltv():
    info_df = pd.read_excel(r'E:\query_result (74).xls')
    print info_df
    info_df['ip'] = info_df['ip'].astype(basestring)
    IP.load(os.path.abspath("./tinyipdata_utf8.dat"))

    def ip_lines():
        for _, row in info_df.iterrows():
            ip = row.ip
            try:
                country = IP.find(ip).strip().encode("utf8")
                if '中国台湾' in country:
                    country = '台湾'
                elif '中国香港' in country:
                    country = '香港'
                elif '中国澳门' in country:
                    country = '澳门'
                elif '中国' in country:
                    country = '中国'
                yield [row.user_id, row.reg_ds, row.ds, row.order_money, row.ip, country]
            except:
                country = ''
                yield [row.user_id, row.reg_ds, row.ds, row.order_money, row.ip, country]
                pass

    result = pd.DataFrame(ip_lines(), columns=['user_id', 'reg_ds', 'ds', 'order_money', 'ip', 'country']).fillna(0)
    print result.head()
    return result


if __name__ == '__main__':
    a = tw_ltv()
    pd.DataFrame(a).to_excel('E:\sanguo_tl_reg_user_pay_info-20170926.xlsx', index=False)
    print "end"


