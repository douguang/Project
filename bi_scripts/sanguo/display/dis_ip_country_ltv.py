#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Hu Chunlong
Description : 注册用户LTV
'''
from utils import hql_to_df, ds_add, ds_delta, format_dates, date_range, update_mysql
import pandas as pd
import settings_dev
from ipip import *

ltv_days = [3, 7, 14, 30, 60]


def formatDate(Date, formatType='YYYY-MM-DD'):
    formatType = formatType.replace('YYYY', Date[0:4])
    formatType = formatType.replace('MM', Date[4:6])
    formatType = formatType.replace('DD', Date[-2:])
    return formatType


def dis_ip_country_ltv(date):
    # 根据要求的LTV天数倒推需要的开始日期
    start_date = ds_add(date, -max(ltv_days) + 1)  # 增加时间判断，11月25日起进行计算
    dates_to_update = [ds_add(date, -ltv_day + 1) for ltv_day in ltv_days]
    server_start_date = settings_dev.start_date.strftime('%Y%m%d')
    f_start_date = formatDate(start_date)
    f_date = formatDate(date)

    # 注册数据
    reg_sql = '''
        select regexp_replace(to_date(reg_time),'-','') as reg_time, account from mid_info_all where ds='{date}'
    '''.format(date=date,)
    reg_df = hql_to_df(reg_sql)
    print reg_df.head()
    ip_sql = '''
        select account,ip from parse_nginx where ds>='{f_start_date}' and ds<='{date}' and account != '' group by account,ip
    '''.format(date=date, f_start_date=f_start_date,)
    ip_df = hql_to_df(ip_sql)
    print ip_df.head()
    ip_df = ip_df.sort_values(by=['account', 'ip', ], ascending=False)
    ip_df = ip_df.drop_duplicates(subset=['account', ], keep='first')
    print ip_df.head()

    reg_ip_df = reg_df.merge(ip_df, on=['account'], how='left').fillna(0)

    # reg_ip_sql = '''
    # select regexp_replace(to_date(reg_time),'-','') as reg_time, account,ip from (
    #     select reg_time, account, ip, row_number() over(partition by account order by reg_time) as rn
    # from mid_info_all
    # where ds = '{date}'
    #   and to_date(reg_time) >= '2017-01-03' and to_date(reg_time) <= '{f_date}') t1 where t1.rn=1
    # '''.format(date=date, f_start_date=f_start_date, f_date=f_date)
    # print reg_ip_sql
    # reg_ip_df = hql_to_df(reg_ip_sql)
    # reg_ip_df.fillna(0)




    reg_ip_df['ip'] = reg_ip_df['ip'].astype(basestring)
    IP.load(os.path.abspath("./tinyipdata_utf8.dat"))

    def ip_lines():
        for _, row in reg_ip_df.iterrows():
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
                yield [row.reg_time, row.account, country]
            except:
                pass
    reg_df = pd.DataFrame(ip_lines(), columns=['ds', 'account', 'country'])
    # reg_df['ds'] = reg_df.ds.str.replace('-', '')
    #print reg_df.head(10)
    # reg_df.to_excel('/home/kaiqigu/Documents/chunlong.xlsx')

    # 所有的充值数据
    pay_sql = '''
    select ds,
           account,
           pay_rmb
    from
    (
        select ds,
               user_id,
               sum(order_money) as pay_rmb
        from raw_paylog
        where platform_2 != 'admin_test' and ds >= '{start_date}' and ds <= '{date}' and order_id not like '%testktwwn%'
        group by ds, user_id
    ) pay
    join
    (
        select user_id, account
        from mid_info_all
        where ds = '{date}'
    ) info on info.user_id = pay.user_id
    '''.format(start_date=start_date, date=date)
    pay_df = hql_to_df(pay_sql)

    # LTV计算主体
    dfs = []
    for date_to_update in dates_to_update:
        days_list = []
        if date_to_update >= '20170103':
            use_reg_df = reg_df[(reg_df.ds == date_to_update)].copy()
            ltv_dfs = use_reg_df.groupby(['ds', 'country']).agg({'account': lambda g: g.nunique()}).reset_index().rename(
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
                    result_ltv_df = result_ltv_df.groupby(['ds', 'country']).agg({
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
                    ltv_dfs = ltv_dfs.merge(
                        result_ltv_df, on=['ds', 'country', 'reg_user_num'])
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
                           for ltv_day in ltv_days), ['ds', 'reg_user_num', 'country'])
            ltv_dfs = ltv_dfs[columns]
            dfs.append(ltv_dfs)
        else:
            continue
    final_result = pd.concat(dfs)
    print final_result
    # 更新MySQL表——每日LTV
    table = 'dis_ip_country_ltv'
    del_sql = 'delete from {0} where ds in {1}'.format(table, format_dates(dates_to_update))
    update_mysql(table, final_result, del_sql)

if __name__ == '__main__':
    for platform in ['sanguo_tl']:
        settings_dev.set_env(platform)
        # date = '20170114'
        # result = dis_ip_country_ltv(date)
        # result.to_excel('/home/kaiqigu/Documents/LTV.xlsx')
        for date in date_range('20170417', '20170418'):
            dis_ip_country_ltv(date)
    print "end"