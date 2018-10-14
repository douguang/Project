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
    select * from dis_card_evo where ds = '20160918' and server = 'kva3'
    '''
    dis_card_evo = test_sqlalchemy(mysql_url, mysql_sql)
    dis_card_equip_num_sql = '''
    select * from dis_card_equip_num where ds = '20160918' and server = 'kva3'
    '''
    dis_card_equip_num = test_sqlalchemy(mysql_url, dis_card_equip_num_sql)
    dis_card_equip_quality_sql = '''
    select * from dis_card_equip_quality where ds = '20160918' and server = 'kva3'
    '''
    dis_card_equip_quality = test_sqlalchemy(mysql_url, dis_card_equip_quality_sql)

    del dis_card_evo['card_name']
    del dis_card_equip_num['card_name']
    del dis_card_equip_quality['card_name']

    dis_card_evo.to_excel('/Users/kaiqigu/Downloads/Excel/dis_card_evo.xlsx')
    dis_card_equip_num.to_excel('/Users/kaiqigu/Downloads/Excel/dis_card_equip_num.xlsx')
    dis_card_equip_quality.to_excel('/Users/kaiqigu/Downloads/Excel/dis_card_equip_quality.xlsx')
