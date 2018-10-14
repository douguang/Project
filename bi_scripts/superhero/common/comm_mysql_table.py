#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Dong Junshuang
Description : 测试表结构 及 各版本中需要的表是否存在
执行脚本方式：run Downloads/bi_scripts/superhero/common/comm_mysql_table.py '20160617'
1. 测试数据库中是否包含此表
2. 测试数据库中表的数据结构是否相同
3. 测试数据库中表中是否有数据
'''

from sqlalchemy.engine import create_engine
import pandas as pd
import sys

table_name = [
'dis_daily_data'
,'dis_daily_keep_rate'
,'dis_vip_level_dst'
,'dis_keep_rate'
,'dis_zombie_distr'
,'dis_act_coinnum_info'
,'dis_card_relive_superhero'
,'dis_card_evo_superhero'
,'dis_card_super_superhero'
,'dis_card_equip_quality_superhero'
,'dis_card_equip_num_superhero'
,'dis_card_use_rate'
,'dis_spend_detail'
,'dis_spend_detail'
,'dis_reg_user_ltv'
]

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

def database_table():
    # 查询数据库中的所有表
    for plat in ['superhero_pub','superhero_ios','superhero_qiku','superhero_vt','superhero_tl']:
    # for plat in ['superhero_pub']:
        mysql_url = 'mysql+pymysql://root:60aa954499f7ab@192.168.1.27/{plat}'.format(plat=plat)
        mysql_sql = "show tables"
        # mysql_sql = "SELECT * FROM dis_pay_platform LIMIT 100"
        table_name = hql_sql(mysql_url, mysql_sql)

if __name__ == '__main__':
    date = sys.argv[1]
    print date
    # database_table()
    table_result = []
    for table in table_name:
        # print table
        for plat in ['superhero_pub','superhero_ios','superhero_qiku','superhero_vt','superhero_tl']:
            mysql_url = 'mysql+pymysql://root:60aa954499f7ab@192.168.1.27/{plat}'.format(plat=plat)
            table_sql = "show tables"
            table_df = hql_sql(mysql_url, table_sql)
            table_list = table_df.xs('Tables_in_{plat}'.format(plat=plat),axis=1).tolist()
            if table in table_list:
                # print table
                # table_sql = "drop table {plat}.{table}".format(table=table,plat=plat)
                # table_df = hql_sql(mysql_url, table_sql)
                col_sql = 'SHOW COLUMNS FROM {table}'.format(table=table)
                # -- data_sql = "SELECT * FROM {table} where ds = '{date}' LIMIT 10".format(table=table,date=date)
                data_sql = "SELECT * FROM {table} LIMIT 10".format(table=table)
                col_df = hql_sql(mysql_url, col_sql)
                data_df = hql_sql(mysql_url, data_sql)
                col_df = col_df.loc[:,['Field','Type']]
                col_df.insert(0,'plat',plat)
                col_df.insert(1,'name',table)
                table_result.append(col_df)
                if data_df.count().ds == 0:
                    print 'the {date} {plat}.{table} is not data'.format(table=table,plat=plat,date=date)
            else:
                print '{table} not in {plat}'.format(table=table,plat=plat)
    # result = pd.concat(table_result)
    # result.to_excel('/Users/kaiqigu/Downloads/Excel/table_columns.xlsx')
    # print '/Users/kaiqigu/Downloads/Excel/table_columns.xlsx is complete!'






