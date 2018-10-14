#!/usr/env/python
# -*- coding:utf-8 -*-
'''
Author      : Lan Xuliu
Description : 卡牌使用率
'''
# import settings_dev
# import pandas as pd
# from utils import ds_add, hql_to_df, update_mysql, get_config
# settings_dev.set_env('dancer_ks')
# detail_config = get_config('character_info')
# for i in detail_config:
#     print i, detail_config.get(i, {}).get('name')
from sqlalchemy.engine import create_engine
import pandas as pd

def hql_sql(url, sql):
    engine = create_engine(url)
    connection = engine.raw_connection()
    cur = connection.cursor()
    cur.execute(sql)
    df = pd.read_sql(sql, connection)
    connection.close()
    return df

table_only = []
for plat in ['superhero_pub', 'superhero_ios', 'superhero_qiku', 'superhero_vt', 'sanguo_ks', 'sanguo_ios', 'sanguo_tw', 'sanguo_tx', 'dancer_ks', 'dancer_tw']:
    mysql_url = 'mysql+pymysql://root:60aa954499f7ab@192.168.1.27/{plat}'.format(
        plat=plat)
    table_sql = "show tables"
    table_df = hql_sql(mysql_url, table_sql)
    table_list = table_df.xs('Tables_in_{plat}'.format(plat=plat), axis=1).tolist()
    for i in table_list:
        if i not in table_only:
            table_only.append(i)
        else:
            continue
print table_only
