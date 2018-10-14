#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Hu Chunlong
Description : 武娘 日常数据
create_date : 2016.07.17
'''
import settings_dev
from utils import ds_add, hql_to_df, update_mysql, date_range


def formatDate(Date, formatType='YYYY-MM-DD'):
    formatType = formatType.replace('YYYY', Date[0:4])
    formatType = formatType.replace('MM', Date[4:6])
    formatType = formatType.replace('DD', Date[-2:])
    return formatType


def dis_daily_data(date):
    reg_date = formatDate(date)
    reg_sql = '''
    SELECT platform,
           count(DISTINCT t1.user_id) AS reg_user_num
    FROM
      (SELECT ds,
              user_id
       FROM parse_info
       WHERE ds = '{date}'
         AND user_id NOT in
           ( SELECT user_id
            FROM mid_info_all
            WHERE ds = '{date_ago}' )) t1
    JOIN
      (SELECT DISTINCT user_id,
                       platform
       FROM parse_actionlog
       WHERE ds = '{date}') t2 ON t1.user_id = t2.user_id
    GROUP BY platform
    '''.format(**{
        'date': date,
        'date_ago': ds_add(date, -1),
    })
    print reg_sql
    dau_sql = au_sql(date, 0, 'dau')
    wau_sql = au_sql(date, -6, 'wau')
    mau_sql = au_sql(date, -29, 'mau')
    pay_sql = order_sql(date)
    new_pay_sql = new_order(date)

    dau_df = hql_to_df(dau_sql)
    wau_df = hql_to_df(wau_sql)
    mau_df = hql_to_df(mau_sql)
    pay_df = hql_to_df(pay_sql)
    new_pay_df = hql_to_df(new_pay_sql)
    reg_df = hql_to_df(reg_sql)

    daily_df = (dau_df.merge(wau_df,
                             on=['platform'],
                             how='outer').merge(mau_df,
                                                on=['platform'],
                                                how='outer').merge(reg_df,
                                                                   on=['platform'],
                                                                   how='outer')
                .merge(pay_df,
                       on=['platform'],
                       how='outer').merge(new_pay_df,
                                          on=['platform'],
                                          how='outer'))

    daily_df['spend_rate'] = daily_df['pay_num'] / daily_df['dau'] * 100
    daily_df['arpu'] = daily_df['income'] / daily_df['dau']
    daily_df['arppu'] = daily_df['income'] / daily_df['pay_num']
    daily_df['ds'] = date

    columns = ['ds', 'platform', 'reg_user_num', 'dau', 'wau', 'mau', 'pay_num',
               'new_pay_num', 'income', 'spend_rate', 'arpu', 'arppu']
    act_daily_df = daily_df[columns].fillna(0)
    # print act_daily_df
    table = 'dis_daily_data'
    print date, table
    # 更新MySQL表
    del_sql = 'delete from {0} where ds="{1}"'.format(table, date)
    update_mysql(table, act_daily_df, del_sql)

    table = 'dis_daily_keep_rate'
    print date, table
    for i in [date, ds_add(date, -1), ds_add(date, -2), ds_add(date, -6)]:
        date = i
        rate_daily_df = rate_info(date)
        # print rate_daily_df
        # 更新MySQL表
        del_sql = 'delete from {0} where ds="{1}"'.format(table, date)
        update_mysql(table, rate_daily_df, del_sql)

    return daily_df


def order_sql(date):
    pay_sql = '''
    SELECT count(t2.user_id) AS pay_num,
           sum(t2.pay_num) AS income,
           t3.platform
    FROM
      ( SELECT user_id ,
               sum(order_money) AS pay_num
       FROM raw_paylog
       WHERE ds = '{date}'
         AND platform_2 <> 'admin_test'
         AND order_id not like '%testktwwn%'
       GROUP BY user_id )t2
    LEFT OUTER JOIN
      ( SELECT distinct user_id,
               platform
       FROM parse_actionlog
       WHERE ds = '{date}'
         AND platform <> 'None' )t3 ON t2.user_id = t3.user_id
    WHERE t2.user_id IS NOT NULL
    GROUP BY t3.platform
    '''.format(**{
        'date': date
    })
    return pay_sql


def new_order(date):
    new_order_sql = '''
    SELECT count(t2.user_id) AS new_pay_num,
           t3.platform
    FROM
      ( SELECT t5.user_id
       FROM
         ( SELECT DISTINCT user_id
          FROM raw_paylog
          WHERE ds = '{date}'
            AND platform_2 <> 'admin_test'
            AND order_id not like '%testktwwn%' )t5
       LEFT OUTER JOIN
         ( SELECT DISTINCT user_id
          FROM raw_paylog
          WHERE ds < '{date}'
            AND platform_2 <> 'admin_test'
            AND order_id not like '%testktwwn%' )t6 ON t5.user_id = t6.user_id
       WHERE t6.user_id IS NULL )t2
    LEFT OUTER JOIN
      ( SELECT distinct user_id,
               platform
       FROM parse_actionlog
       WHERE ds = '{date}'
         AND platform <> 'None' )t3 ON t2.user_id = t3.user_id
    GROUP BY t3.platform
    '''.format(**{
        'date': date
    })
    return new_order_sql


def au_sql(date, num, name):
    # 将raw_activeuser改为了parse_info
    au_sql = '''
    SELECT t3.platform ,
           count(t1.user_id) AS '{name}'
    FROM
      ( SELECT DISTINCT user_id
       FROM parse_info
       WHERE ds >= '{date_au}'
         AND ds <= '{date}' )t1
    JOIN
      ( SELECT distinct user_id,
               platform
       FROM parse_actionlog
       WHERE ds >= '{date_au}'
         AND ds <= '{date}'
         AND platform <> 'None' )t3 ON t1.user_id = t3.user_id
    GROUP BY t3.platform
        '''.format(**{
        'date': date,
        'date_au': ds_add(date, num),
        'name': name,
    })
    return au_sql


def preserve_rate(date):
    # 将raw_registeruser更改为parse_info
    reg_date = formatDate(date)
    sql = '''
    SELECT platform ,
           sum(CASE WHEN NEW.user_id IS NULL THEN 0 ELSE 1 END) AS reg_user_num ,
           sum(CASE WHEN NEW.user_id IS NULL
               OR d2.user_id IS NULL THEN 0 ELSE 1 END) AS d2 ,
           sum(CASE WHEN NEW.user_id IS NULL
               OR d3.user_id IS NULL THEN 0 ELSE 1 END) AS d3 ,
           sum(CASE WHEN NEW.user_id IS NULL
               OR d7.user_id IS NULL THEN 0 ELSE 1 END) AS d7
    FROM
      ( SELECT user_id
       FROM parse_info
       WHERE ds = '{date}'
         AND user_id NOT in
           ( SELECT user_id
            FROM mid_info_all
            WHERE ds = '{date_ago}') ) t1
    LEFT OUTER JOIN
      ( SELECT distinct user_id,
               platform
        FROM parse_actionlog
        WHERE ds = '{date}'
          AND platform <> 'None' ) t2 ON t1.user_id = t2.user_id
    LEFT OUTER JOIN
      ( SELECT user_id
       FROM parse_info
       WHERE ds = '{date}'
         AND user_id NOT in
           ( SELECT user_id
            FROM mid_info_all
            WHERE ds = '{date_ago}') ) NEW ON t1.user_id = NEW.user_id
    LEFT OUTER JOIN
      ( SELECT user_id
       FROM parse_info
       WHERE ds = '{d2}' ) d2 ON t1.user_id = d2.user_id
    LEFT OUTER JOIN
      ( SELECT user_id
       FROM parse_info
       WHERE ds = '{d3}' ) d3 ON t1.user_id = d3.user_id
    LEFT OUTER JOIN
      ( SELECT user_id
       FROM parse_info
       WHERE ds = '{d7}' ) d7 ON t1.user_id = d7.user_id
    GROUP BY platform
    '''.format(**{
        'date': date,
        'date_ago': ds_add(date, -1),
        'd2': ds_add(date, 2 - 1),
        'd3': ds_add(date, 3 - 1),
        'd7': ds_add(date, 7 - 1),
    })
    return sql


def rate_info(date):
    # 用户留存率
    preserve_sql = preserve_rate(date)
    preserve_df = hql_to_df(preserve_sql)

    for i in [2, 3, 7]:
        preserve_df['d%s_keeprate' % i] = preserve_df['d%s' %
                                                      i] / preserve_df['reg_user_num']
    preserve_df['ds'] = date

    columns = ['ds', 'platform', 'reg_user_num',
               'd2_keeprate', 'd3_keeprate', 'd7_keeprate']
    rate_daily_df = preserve_df[columns]

    return rate_daily_df

if __name__ == '__main__':

    # for platform in ['dancer_tw', 'dancer_pub']:
    #     settings_dev.set_env(platform)
    #     for date in date_range('20170119', '20170121'):
    #         dis_daily_data(date)
    settings_dev.set_env('dancer_tw')
    dis_daily_data('20170119')
