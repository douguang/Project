#!/usr/bin/env python
# -*- coding: UTF-8 -*-
"""
Author      : Hu Chunlong
Description : 用户转化率
create_date : 2016.12.29
"""
import settings_dev
from utils import update_mysql, hql_to_df, ds_add
from sqlalchemy.engine import create_engine


def dis_nginx_user_exchange(date):
    # method列表：
    sql = '''
    select {date} as ds, method, pt_chl, count(distinct user_token) as user_num
    from parse_nginx
    where ds = '{date}'
      and method in ('loading', 'all_config', 'cards.open', 'user.main_page', 'user.guide')
      and pt_chl != ''
      and user_token not in (
      select user_id as user_token
      from mid_info_all
      where ds = '{date_1}'
      )
    group by method, pt_chl
    '''.format(**{
        'date': date,
        'date_1': ds_add(date, -1),
    })
    df = hql_to_df(sql)
    print df
    result_df = df.pivot_table('user_num', ['ds', 'pt_chl'], 'method').rename(columns={
                        'cards.open': 'cards_open',
                        'user.main_page': 'user_main_page',
                        'user.guide': 'user_guide'}).reset_index()
    columns = ['ds', 'pt_chl', 'loading', 'all_config', 'cards_open', 'user_main_page', 'user_guide']
    result_df = result_df[columns]
    print result_df

    table = 'dis_nginx_user_exchange_back'
    del_sql = "DROP TABLES {0}".format(table)
    update_mysql(table, result_df, del_sql)

    url = "mysql+pymysql://root:60aa954499f7ab@192.168.1.27/dancer_pub"
    engine = create_engine(url)
    connection = engine.raw_connection()
    insert_sql = "insert into dis_nginx_user_exchange SELECT * FROM dis_nginx_user_exchange_back"
    cur = connection.cursor()
    cur.execute(insert_sql)
    connection.commit()
    print 'insert'
    connection.close()
    # use_table = 'dis_nginx_user_exchange'
    # del_sql = "insert into {0} SELECT * FROM {1}".format(use_table, table)
    # update_mysql(use_table, '', del_sql)


if __name__ == '__main__':
    date = '20170103'
    settings_dev.set_env('dancer_pub')
    dis_nginx_user_exchange(date)