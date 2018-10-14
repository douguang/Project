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


def test_sqlalchemy(url, sql):
    engine = create_engine(url)
    connection = engine.raw_connection()
    # 原始方法
    cur = connection.cursor()
    cur.execute(sql)
    # print cur.fetchone()
    # print cur.fetchall()
    # 读取为pandas dataframe
    df = pd.read_sql(sql, connection)
    print df
    connection.close()


if __name__ == '__main__':
    #mysql_url = 'mysql+pymysql://root:60aa954499f7ab@192.168.1.27/sanguo'
    # mysql_sql = "SELECT * FROM dis_pay_platform LIMIT 100"
    # test_sqlalchemy(mysql_url, mysql_sql)

    hql = "SELECT * FROM raw_info LIMIT 10"
    # impala connector 由 [impyla](https://github.com/cloudera/impyla) 提供
    # hive_url = 'hive://192.168.1.8:10000/superhero_bi'
    # test_sqlalchemy(hive_url, hql)
    impala_url = 'impala://192.168.1.47:21050/superhero_bi'
    result = test_sqlalchemy(impala_url, hql)
