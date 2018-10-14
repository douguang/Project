#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
@Time    : 17-7-31 下午5:35
@Author  : Andy 
@File    : dis_country_language_ltv.py
@Software: PyCharm
Description :   分语言分国家
'''

from utils import hql_to_df, ds_add, ds_delta, format_dates, date_range, update_mysql
import pandas as pd
import settings_dev
from ipip import *

ltv_days = [1,2,3,4,5,6,7,14,30,60,90,120,150,180]


def formatDate(Date, formatType='YYYY-MM-DD'):
    formatType = formatType.replace('YYYY', Date[0:4])
    formatType = formatType.replace('MM', Date[4:6])
    formatType = formatType.replace('DD', Date[-2:])
    return formatType


def dis_country_language_ltv(date):
    # 根据要求的LTV天数倒推需要的开始日期
    start_date = ds_add(date, -max(ltv_days) + 1)  # 增加时间判断，11月25日起进行计算
    dates_to_update = [ds_add(date, -ltv_day + 1) for ltv_day in ltv_days]
    server_start_date = settings_dev.start_date.strftime('%Y%m%d')
    f_start_date = formatDate(start_date)
    f_date = formatDate(date)
    # 注册数据
    reg_ip_sql = '''
    select regexp_replace(to_date(t1.reg_time),'-','') AS reg_time,t1.account,t2.ip as regist_ip,t2. language as language_sort from (
    select account,min(reg_time) as reg_time from mid_info_all where ds='{date}'and account != ''group by account
    )t1 left outer join (
    select account,reg_time,ip,language from mid_info_all where ds='{date}' group by account,reg_time,ip,language
      )t2 on t1.account=t2.account and t1.reg_time=t2.reg_time
      where t1.reg_time >= '2017-07-30 00:00:00' and t2.ip != ''
      AND to_date(t1.reg_time) >= '{f_start_date}'
      AND to_date(t1.reg_time) <= '{f_date}'
      group by reg_time,t1.account,regist_ip,language_sort
    '''.format(date=date, f_start_date=f_start_date, f_date=f_date)
    print reg_ip_sql
    reg_ip_df = hql_to_df(reg_ip_sql)
    reg_ip_df.fillna(0)
    print reg_ip_df.head()
    reg_ip_df['regist_ip'] = reg_ip_df['regist_ip'].astype(basestring)
    IP.load(os.path.abspath("./tinyipdata_utf8.dat"))

    def ip_lines():
        for _, row in reg_ip_df.iterrows():
            ip = row.regist_ip
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
                yield [row.reg_time, row.account, country,row.language_sort]
            except:
                pass
    reg_df = pd.DataFrame(ip_lines(), columns=['ds', 'account', 'country','language_sort'])
    language_dic = {None: '泰语', '0': '英文', '1': '简中', '2': '繁中', '3': '泰语', '4': '越南语', '5': '印尼语'}
    reg_df['language_sort'] = reg_df.replace(language_dic).language_sort
    print reg_df.head()

    # 所有的充值数据
    pay_sql = '''
    SELECT ds,
           account,
           pay_rmb
    FROM
      ( SELECT ds,
               user_id,
               sum(order_money) AS pay_rmb
       FROM raw_paylog
       WHERE platform_2 != 'admin_test'
         AND platform_2 != 'admin'
         AND ds >= '{start_date}'
         AND ds <= '{date}'
         AND order_id NOT LIKE '%test%'
       GROUP BY ds,
                user_id ) pay
    JOIN
      ( SELECT user_id,
               account
       FROM mid_info_all
       WHERE ds = '{date}' ) info ON info.user_id = pay.user_id
    '''.format(start_date=start_date, date=date)
    pay_df = hql_to_df(pay_sql)
    print pay_df.head()
    # LTV计算主体
    dfs = []
    for date_to_update in dates_to_update:
        days_list = []
        if date_to_update >= '20170730':
            use_reg_df = reg_df[(reg_df.ds == date_to_update)].copy()
            ltv_dfs = use_reg_df.groupby(['ds', 'country','language_sort',]).agg({'account': lambda g: g.nunique()}).reset_index().rename(
                columns={
                    'account': 'reg_user_num'
                })

            for ltv_ds in ltv_days:
                ltv_end_date = ds_add(date_to_update, ltv_ds - 1)
                if ltv_end_date <= date:
                    use_pay_df = pay_df[(pay_df.ds <= ltv_end_date) & (
                        pay_df.ds >= date_to_update)][['account', 'pay_rmb']]
                    use_pay_df['pay_num'] = use_pay_df['account']
                    result_ltv_df = use_reg_df.merge(
                        use_pay_df, on='account', how='left')
                    result_ltv_df = result_ltv_df.groupby(['ds', 'country','language_sort',]).agg({
                        'account': lambda g: g.nunique(),
                        'pay_num': lambda g: g.nunique(),
                        'pay_rmb': lambda g: g.sum()
                    }).reset_index().rename(columns={
                        'account': 'reg_user_num',
                        'pay_num': 'd%s_pay_num' % ltv_ds,
                        'pay_rmb': 'd%s_pay_rmb' % ltv_ds
                    }).fillna(0)
                    result_ltv_df['d%s_ltv' % ltv_ds] = result_ltv_df['d%s_pay_rmb' % ltv_ds] * 1.0 / result_ltv_df[
                        'reg_user_num']
                    print '------------------'
                    print ltv_dfs.head()
                    print result_ltv_df.head()
                    ltv_dfs = ltv_dfs.merge(
                        result_ltv_df, on=['ds','country','language_sort','reg_user_num',])
                else:
                    days = ds_delta(date_to_update, ltv_end_date) + 1
                    days_list.append(days)
                    for ltv_date in ltv_days:
                        if ltv_date in days_list:
                            ltv_dfs['d%s_pay_num' % ltv_date] = 0
                            ltv_dfs['d%s_pay_rmb' % ltv_date] = 0
                            ltv_dfs['d%s_ltv' % ltv_date] = 0
            # print ltv_dfs
            columns = sum((['d%d_pay_num' % ltv_day, 'd%d_ltv' % ltv_day, ]
                           for ltv_day in ltv_days), ['ds', 'country','language_sort','reg_user_num', ])
            ltv_dfs = ltv_dfs[columns]
            dfs.append(ltv_dfs)
        else:
            continue
    final_result = pd.concat(dfs)

    # 更新MySQL表——每日LTV
    table = 'dis_country_language_ltv'
    del_sql = 'delete from {0} where ds in {1}'.format(
        table, format_dates(dates_to_update))
    update_mysql(table, final_result, del_sql)

if __name__ == '__main__':
    for platform in ['sanguo_tl']:
        settings_dev.set_env(platform)
        for date in date_range('20170808', '20170814'):
            dis_country_language_ltv(date)
