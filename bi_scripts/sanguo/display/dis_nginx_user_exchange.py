#!/usr/bin/env python
# -*- coding: UTF-8 -*-
"""
Author      : Hu Chunlong
Description : 用户转化率
create_date : 2016.12.29
"""
import settings_dev
from utils import update_mysql, hql_to_df, ds_add, date_range
from sqlalchemy.engine import create_engine


def dis_nginx_user_exchange(date, platform):
    settings_dev.set_env(platform)
    # method列表：
    sql = '''
    select {date} as ds, method, platform_channel as pt_chl, count(distinct user_token) as user_num
    from parse_nginx
    where ds = '{date}'
      and method in ('loading', 'all_config', 'cards.open', 'user.main_page', 'user.guide')
      and platform_channel != ''
      and user_token not in (
      select user_id as user_token
      from mid_info_all
      where ds = '{date_1}'
      )
    group by method, platform_channel
    '''.format(**{
        'date': date,
        'date_1': ds_add(date, -1),
    })
    df = hql_to_df(sql)
    result_df = df.pivot_table('user_num', ['ds', 'pt_chl'], 'method').rename(columns={
                        'cards.open': 'cards_open',
                        'user.main_page': 'user_main_page',
                        'user.guide': 'user_guide'}).reset_index()
    print result_df
    columns = ['ds', 'pt_chl', 'loading', 'all_config', 'cards_open', 'user_main_page', 'user_guide']
    result_df = result_df[columns]
    print result_df

    table = 'dis_nginx_user_exchange_back'
    del_sql = "DROP TABLES {0}".format(table)
    update_mysql(table, result_df, del_sql)

    url = "mysql+pymysql://root:60aa954499f7ab@192.168.1.27/{0}".format(platform)
    engine = create_engine(url)
    connection = engine.raw_connection()
    insert_sql = "insert into dis_nginx_user_exchange SELECT * FROM dis_nginx_user_exchange_back"
    cur = connection.cursor()
    cur.execute(insert_sql)
    connection.commit()
    print 'insert'
    connection.close()

if __name__ == '__main__':
    date = '20170103'
    # for date in date_range('20161222', '20161229'):
    for platform in ['sanguo_ks', 'sanguo_tw']:
        dis_nginx_user_exchange(date, platform)