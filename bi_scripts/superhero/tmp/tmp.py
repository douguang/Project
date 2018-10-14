#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
description:
1.link to mysql
2.link to impala
3.link to hive
'''

from sqlalchemy.engine import create_engine
import pandas as pd

if __name__ == '__main__':
    mysql_url = 'mysql+pymysql://root:60aa954499f7ab@192.168.1.27/sanguo_tw'
    mysql_sql = '''
    SELECT *
    FROM dis_common_uid_level_raw
    WHERE ds >= '20160618'
    and ds <= '20160624'
    '''
    # test_sqlalchemy(mysql_url, mysql_sql)
    # def test_sqlalchemy(url, sql):
    engine = create_engine(mysql_url)
    connection = engine.raw_connection()
    # 原始方法
    cur = connection.cursor()
    cur.execute(mysql_sql)
    #print cur.fetchone()
    #print cur.fetchall()
    # 读取为pandas dataframe
    df = pd.read_sql(mysql_sql, connection)
    print df
    connection.close()

    # data = df[df['ds'] == '20160619']

    info_agg_df = df.groupby('ds').agg({
        'd8_level': lambda c: c.mean(),
        'd15_level': lambda c: c.mean(),
        'd31_level': lambda c: c.mean(),
    }).reset_index()

    # 'life_day': {
    #         'mean': lambda c: c.mean(),
    #         'median': lambda c: c.median(),
    #     }
