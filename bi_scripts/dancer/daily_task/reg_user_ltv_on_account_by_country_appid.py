#!/usr/bin/env python  
# encoding: utf-8  

""" 
@author: Andy 
@site:  
@software: PyCharm 
@file: reg_user_ltv_on_account_by_country_appid.py 
@time: 18/1/22 下午3:29 
"""

from utils import date_range, ds_add, hql_to_df
import settings_dev
import pandas as pd
from ipip import *

def data_reduce(start_ds,end_ds,n_ltv):
    print start_ds,end_ds,n_ltv

    dates = list(date_range(start_ds, end_ds))
    print list(dates)
    ltv_days = range(1, n_ltv + 1)

    reg_sql = '''
        select t1.account,t1.ds as reg_ds,t2.user_id,t2.appid,t2.regist_ip  as ip from (
          select account,min(regexp_replace(substring(reg_time,1,10),'-','')) as ds from mid_info_all where ds='{end_ds}' and regexp_replace(substring(reg_time,1,10),'-','') !='19700101' group by account
        )t1 left outer join(
          select account,user_id,appid,regist_ip from mid_info_all where ds='{end_ds}' group by account,user_id,appid,regist_ip
        )t2 on t1.account=t2.account
        group by t1.account,t1.ds,t2.user_id,t2.appid,t2.regist_ip
    '''.format(start_ds=start_ds, end_ds=end_ds)
    print reg_sql
    reg_df = hql_to_df(reg_sql)
    print reg_df.head()

    pay_sql = '''
        select ds,user_id,sum(order_rmb) as order_money from raw_paylog where ds>='{start_ds}' and ds<='{end_ds}' and platform_2 != 'admin_test' group by ds,user_id
    '''.format(start_ds=start_ds, end_ds=ds_add(end_ds,n_ltv-1))
    print pay_sql
    pay_df = hql_to_df(pay_sql)
    print pay_df

    reg_df['ip'] = reg_df['ip'].astype(basestring)
    IP.load(os.path.abspath("./tinyipdata_utf8.dat"))

    def ip_lines():
        for _, row in reg_df.iterrows():
            ip = row.ip
            country = ''
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
            except:
                pass
            yield [row.account, row.reg_ds,row.user_id,row.appid,row.ip,country]

    reg_df = pd.DataFrame(ip_lines(), columns=['account', 'reg_ds','user_id', 'appid','ip', 'country'])
    print reg_df.head()

    pay_df = pay_df.merge(reg_df[['reg_ds','user_id','account','appid','country']],on=['user_id',],how='left')
    print pay_df.head()

    # 计算LTV
    daily_ltv_dfs = []
    for date in dates:
        reg_daily_df = reg_df.loc[reg_df.reg_ds == date].copy()
        reg_daily_mid = reg_daily_df.groupby(['reg_ds', 'appid', 'country']).agg({
            'account': lambda g: g.nunique(),
        }).reset_index().rename(columns={'account':'reg_num',})
        print 'reg_daily_mid'
        print reg_daily_mid.head()
        pay_daily_df = pay_df.loc[pay_df.reg_ds == date].copy()
        pay_daily_mid = pay_daily_df.groupby(['appid', 'country', 'ds']).agg({
            'order_money': 'sum',
        }).reset_index().rename(columns={'order_money': 'order_money', })
        print 'pay_daily_mid'
        print pay_daily_mid.head()
        if pay_daily_mid.__len__() != 0:
            for ltv_day in ltv_days:
                ltv_date = ds_add(date, ltv_day - 1)
                if ltv_date <= end_ds:
                    ltv_range_pay_df = pay_daily_mid.loc[(pay_daily_mid.ds >= date) & (pay_daily_mid.ds <= ltv_date)][
                        ['appid', 'country','order_money']].groupby(['appid','country']).sum().reset_index().rename(columns={'order_money': 'd%d_order_money' % ltv_day})
                else:
                    ltv_range_pay_df = pay_daily_mid.loc[(pay_daily_mid.ds >= date) & (pay_daily_mid.ds <= ltv_date)][
                        ['appid', 'country', 'order_money']].groupby(['appid', 'country']).sum().reset_index()
                    ltv_range_pay_df['order_money'] = 0
                    ltv_range_pay_df = ltv_range_pay_df.rename(columns={'order_money': 'd%d_order_money' % ltv_day})

                reg_daily_mid = pd.DataFrame(reg_daily_mid).merge(ltv_range_pay_df, on=['appid', 'country', ],
                                                                  how='left').fillna(0)
            daily_ltv_dfs.append(reg_daily_mid)

    result_df = pd.concat(daily_ltv_dfs).reset_index()
    print result_df
    for ltv_day in ltv_days:
        result_df['d%d_ltv' % ltv_day] = result_df[
            'd%d_order_money' % ltv_day] / result_df.reg_num

    print result_df
    result_df.to_excel(r'E:\dancer_tw_on_account_by_appid-country_ltv-20180122.xlsx', index=False)

if __name__ == '__main__':
    settings_dev.set_env('dancer_tw')
    data_reduce('20170916','20180121',90)


