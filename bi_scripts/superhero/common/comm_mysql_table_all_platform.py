#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Dong Junshuang
Description : 测试表结构 及 各版本中需要的表是否存在
1. 测试数据库中是否包含此表
2. 测试数据库中表的数据结构是否相同
3. 测试数据库中表中是否有数据
'''

from sqlalchemy.engine import create_engine
import pandas as pd

platform_list = ['superhero_pub', 'superhero_ios', 'superhero_qiku', 'superhero_vt', 'sanguo_ks', 'sanguo_ios', 'sanguo_tw', 'sanguo_tx', 'dancer_ks', 'dancer_tw']

def hql_sql(url, sql):
    engine = create_engine(url)
    connection = engine.raw_connection()
    # 原始方法
    cur = connection.cursor()
    cur.execute(sql)
    # 读取为pandas dataframe
    df = pd.read_sql(sql, connection)
    # print df
    connection.close()
    return df

table_name = []
for plat in platform_list:
    mysql_url = 'mysql+pymysql://root:60aa954499f7ab@192.168.1.27/{plat}'.format(
        plat=plat)
    table_sql = "show tables"
    table_df = hql_sql(mysql_url, table_sql)
    table_list = table_df.xs('Tables_in_{plat}'.format(plat=plat), axis=1).tolist()
    for i in table_list:
        if i not in table_name:
            if i.startswith('dis_'):
                table_name.append(i)
        else:
            continue

def database_table():
    # 查询数据库中的所有表
    for plat in platform_list:
        # for plat in ['superhero_pub']:
        mysql_url = 'mysql+pymysql://root:60aa954499f7ab@192.168.1.27/{plat}'.format(
            plat=plat)
        mysql_sql = "show tables"
        # mysql_sql = "SELECT * FROM dis_pay_platform LIMIT 100"
        table_name = hql_sql(mysql_url, mysql_sql)

if __name__ == '__main__':
    # database_table()
    table_result = []
    for table in table_name:
        # print table
        for plat in platform_list:
            mysql_url = 'mysql+pymysql://root:60aa954499f7ab@192.168.1.27/{plat}'.format(
                plat=plat)
            table_sql = "show tables"
            table_df = hql_sql(mysql_url, table_sql)
            table_list = table_df.xs(
                'Tables_in_{plat}'.format(plat=plat), axis=1).tolist()
            if table in table_list:
                col_sql = 'SHOW COLUMNS FROM {table}'.format(table=table)
                col_df = hql_sql(mysql_url, col_sql)
                col_df = col_df.loc[:, ['Field', 'Type']]
                col_df.insert(0, 'plat', plat)
                col_df.insert(1, 'name', table)
                # print col_df
                table_result.append(col_df)
            else:
                print '{table} not in {plat}'.format(table=table, plat=plat)
    result = pd.concat(table_result)
    result.to_excel('/Users/kaiqigu/Downloads/table_columns.xlsx')
    print '/Users/kaiqigu/Downloads/table_columns.xlsx is complete!'
