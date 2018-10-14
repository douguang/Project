#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
@Time    : 2017/10/11 0011 16:40
@Author  : Andy 
@File    : reg_country_android-apple_ltv_on_account.py
@Software: PyCharm
Description :
'''


from utils import date_range, ds_add, hql_to_df
import settings_dev
import pandas as pd
from ipip import *
import sys
reload(sys)
sys.setdefaultencoding("utf-8")


def data_reduce(start_ds,end_ds,n_ltv):
    print start_ds,end_ds,n_ltv

    dates = list(date_range(start_ds, end_ds))
    print list(dates)
    ltv_days = range(1, n_ltv + 1)

    reg_sql = '''
        select t1.account,t1.ds as reg_ds,t2.user_id,t2.language from (
          select account,min(regexp_replace(substring(reg_time,1,10),'-','')) as ds from mid_info_all where ds='{end_ds}' and regexp_replace(substring(reg_time,1,10),'-','') !='19700101' group by account
        )t1 left outer join(
          select account,user_id,platform as language from mid_info_all where ds='{end_ds}' group by account,user_id,language
        )t2 on t1.account=t2.account
        group by t1.account,t1.ds,t2.user_id,t2.language
    '''.format(start_ds=start_ds, end_ds=end_ds)
    print reg_sql
    reg_df = hql_to_df(reg_sql)
    reg_df = reg_df[['account','reg_ds','user_id',]]
    # rep_dic = {None: '泰语', '0': '英文', '1': '简中', '2': '繁中', '3': '泰语', '4': '越南语', '5': '印尼语'}
    # reg_df['language'] = reg_df.replace(rep_dic).language
    print reg_df

    # 设备
    equipment_sql = '''
      select user_id,case when identifier  like '%appstore%' then 'apple' else 'android' end as language
      from user_identifier_info
      where ds = '{end_ds}'
      group by user_id,case when identifier  like '%appstore%' then 'apple' else 'android' end
    '''.format(start_ds=start_ds, end_ds=end_ds)
    print equipment_sql
    equipment_df = hql_to_df(equipment_sql)
    print equipment_df.head()
    reg_mid_df = reg_df.merge(equipment_df, on=['user_id', ], how='left')
    equipment_df = reg_mid_df[['account','language']].drop_duplicates(subset=['account', ], keep='first')
    reg_df = reg_df.merge(equipment_df, on=['account', ], how='left')
    reg_df['language'] = reg_df.language.fillna('android')
    print reg_df

    pay_sql = '''
        select ds,user_id,sum(order_money) as order_money from raw_paylog where ds>='{start_ds}' and ds<='{end_ds}' and platform_2 != 'admin_test' group by ds,user_id
    '''.format(start_ds=start_ds, end_ds=ds_add(end_ds,n_ltv-1))
    print pay_sql
    pay_df = hql_to_df(pay_sql)
    print pay_df

    # 国际版分国家
    ip_sql = '''
         select user_id,ip from user_identifier_info  where ds='{end_ds}' group by user_id,ip
    '''.format(start_ds=start_ds, end_ds=end_ds)
    print ip_sql
    ip_df = hql_to_df(ip_sql)
    print ip_df
    ip_df = ip_df.sort_values(by=['user_id', 'ip', ], ascending=False)
    ip_df = ip_df.drop_duplicates(subset=['user_id', ], keep='first')

    ip_df['ip'] = ip_df['ip'].astype(basestring)
    IP.load(os.path.abspath("./tinyipdata_utf8.dat"))

    def ip_lines():
        for _, row in ip_df.iterrows():
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
                yield [row.user_id, country]
            except:
                pass

    country_df = pd.DataFrame(ip_lines(), columns=['user_id', 'country'])
    print country_df.head()

    reg_df = reg_df.merge(country_df,on=['user_id',],how='left')
    print reg_df
    pay_df = pay_df.merge(reg_df[['reg_ds','user_id','account','language','country']],on=['user_id',],how='left')
    print pay_df

    # 计算LTV
    daily_ltv_dfs = []
    for date in dates:
        reg_daily_df = reg_df.loc[reg_df.reg_ds == date].copy()
        reg_daily_mid = reg_daily_df.groupby(['reg_ds', 'language', 'country']).agg({
            'account': lambda g: g.nunique(),
        }).reset_index().rename(columns={'account':'reg_num',})
        print 'reg_daily_mid'
        print reg_daily_mid.head()
        if reg_daily_mid.__len__() == 0:
            reg_daily_mid = pd.DataFrame(columns=['','',])
        pay_daily_df = pay_df.loc[pay_df.reg_ds == date].copy()
        pay_daily_mid = pay_daily_df.groupby(['language', 'country', 'ds']).agg({
            'order_money': 'sum',
        }).reset_index().rename(columns={'order_money': 'order_money', })
        print 'pay_daily_mid'
        print pay_daily_mid.head()
        for ltv_day in ltv_days:
            ltv_date = ds_add(date, ltv_day - 1)
            if ltv_date <= end_ds:
                ltv_range_pay_df = pay_daily_mid.loc[(pay_daily_mid.ds >= date) & (pay_daily_mid.ds <= ltv_date)][
                    ['language', 'country','order_money']].groupby(['language','country']).sum().reset_index().rename(columns={'order_money': 'd%d_order_money' % ltv_day})

                print 'ltv_range_pay_df'
                print ltv_range_pay_df.head()
            else:
                ltv_range_pay_df = pay_daily_mid.loc[(pay_daily_mid.ds >= date) & (pay_daily_mid.ds <= ltv_date)][
                    ['language', 'country', 'order_money']].groupby(['language', 'country']).sum().reset_index()
                ltv_range_pay_df['order_money'] = 0
                ltv_range_pay_df = ltv_range_pay_df.rename(columns={'order_money': 'd%d_order_money' % ltv_day})
                print "reg_daily_mid2"
                print reg_daily_mid.head()

            reg_daily_mid = pd.DataFrame(reg_daily_mid).merge(ltv_range_pay_df, on=['language', 'country', ],
                                                              how='left').fillna(0)
            print "reg_daily_mid"
            print reg_daily_mid.head()
        daily_ltv_dfs.append(reg_daily_mid)

    result_df = pd.concat(daily_ltv_dfs).reset_index()
    print result_df
    for ltv_day in ltv_days:
        result_df['d%d_ltv' % ltv_day] = result_df[
            'd%d_order_money' % ltv_day] / result_df.reg_num

    print result_df
    result_df.to_excel('/Users/kaiqigu/Documents/Sanguo/sanguo_tl_android-apple_on_country_ltv_on_account-20180330-2.xlsx', index=False,)


if __name__ == '__main__':
    settings_dev.set_env('sanguo_tl')
    data_reduce('20180101','20180329',90)


