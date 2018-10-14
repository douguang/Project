#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
'''

from sqlalchemy.engine import create_engine
import pandas as pd

def test_sqlalchemy(url, sql):
    engine = create_engine(url)
    connection = engine.raw_connection()
    # 原始方法
    cur = connection.cursor()
    cur.execute(sql)
    df = pd.read_sql(sql, connection)
    connection.close()
    return df

if __name__ == '__main__':
    mysql_url = 'mysql+pymysql://root:60aa954499f7ab@192.168.1.27/superhero_self_en'
    mysql_sql = '''
    select * from dis_spend_detail where ds ='20160919' and server = 'kva3'
    '''
    dis_spend_detail_df = test_sqlalchemy(mysql_url, mysql_sql)
    coin_mysql_sql = '''
    select * from dis_spend_detail_coin_shop where ds ='20160919' and server = 'kva3'
    '''
    dis_spend_detail_coin_shop_df = test_sqlalchemy(mysql_url, coin_mysql_sql)
    quota_mysql_sql = '''
    select * from dis_spend_detail_quota_shop where ds ='20160919' and server = 'kva3'
    '''
    dis_spend_detail_quota_shop_df = test_sqlalchemy(mysql_url, quota_mysql_sql)

    dis_spend_detail_df.to_excel('/Users/kaiqigu/Downloads/Excel/dis_spend_detail.xlsx')
    dis_spend_detail_coin_shop_df.to_excel('/Users/kaiqigu/Downloads/Excel/dis_spend_detail_coin_shop_df.xlsx')
    dis_spend_detail_quota_shop_df.to_excel('/Users/kaiqigu/Downloads/Excel/dis_spend_detail_quota_shop_df.xlsx')
