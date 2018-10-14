#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Hu Wenchao
Description : 开服前四周小R LTV
'''
from utils import hql_to_df, get_server_list, ds_add, date_range, format_dates, update_mysql
import pandas as pd
import datetime
import settings_dev

recharge_limits = [0, 100, 500, 1000]

def dis_first_month_ltv(date):
    reg_date = ds_add(date, -27)
    reg_date_3day = ds_add(reg_date, 2)
    ltv_weeks = [1, 2, 3, 4]

    # 把28天前开服的服务器筛选出来
    servers_to_update = {}
    d = get_server_list()
    for k, v in d.iteritems():
        open_date = datetime.datetime.fromtimestamp(v).strftime('%Y%m%d')
        if open_date == reg_date:
            print k, open_date
            servers_to_update[k] = open_date
    if not servers_to_update:
        return

    reg_sql = '''
    select ds, user_id
    from raw_registeruser
    where ds >= '{reg_date}' and ds <= '{reg_date_3day}'
    '''.format(reg_date=reg_date, reg_date_3day=reg_date_3day)
    reg_df = hql_to_df(reg_sql)
    reg_df['server'] = reg_df.user_id.map(lambda s: s[:-7])

    pay_sql = '''
    select ds,
           user_id,
           sum(order_money) as pay_rmb
    from raw_paylog
    where platform_2 != 'admin_test' and ds >= '{reg_date}' and ds <= '{date}'
    group by ds, user_id
    '''.format(reg_date=reg_date, date=date)
    pay_df = hql_to_df(pay_sql)
    pay_daily_df = pay_df.pivot_table('pay_rmb', ['user_id'], 'ds').reset_index().fillna(0)

    def yield_result():
        for server, server_open_date in servers_to_update.iteritems():
            server_uids_pay = reg_df[reg_df.server == server][['user_id']].merge(pay_daily_df, how='left').fillna(0)
            reg_num = server_uids_pay.user_id.count()
            for week in ltv_weeks:
                date_to_sum = list(date_range(server_open_date, ds_add(server_open_date, week * 7 - 1)))
                server_uids_pay['week%d_pay' % week] = server_uids_pay[date_to_sum].sum(axis=1)
            for limit in recharge_limits:
                weekly_ltvs = []
                for week in ltv_weeks:
                    c = 'week%d_pay' % week
                    if limit == 0:
                        weekly_ltv = server_uids_pay[c].mean()
                    else:
                        weekly_ltv = server_uids_pay[server_uids_pay[c] < week * limit][c].mean()
                    weekly_ltvs.append(weekly_ltv)
                # print [server, server_open_date, reg_num, limit] + weekly_ltvs
                yield [server, server_open_date, reg_num, limit] + weekly_ltvs

    result_df = pd.DataFrame(yield_result(), columns=['server', 'server_open_date', 'reg_num', 'small_r_limit'] + ['week%d_pay' % week for week in ltv_weeks])
    print result_df

    # 更新MySQL表
    table = 'dis_first_month_ltv'
    del_sql = 'delete from {0} where server in {1}'.format(table, format_dates(servers_to_update))
    print del_sql
    update_mysql(table, result_df, del_sql)

if __name__ == '__main__':
    for platform in ['metal_test',]:
        settings_dev.set_env(platform)
        for date in date_range('20170307', '20170314'):
            dis_first_month_ltv(date)
    print "end"
