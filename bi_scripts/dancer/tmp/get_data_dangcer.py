#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
description:
Author: BI
Funciton: Link impala
'''

from sqlalchemy.engine import create_engine
import pandas as pd
import time

def impala_sql(game,sql):
    impala_url = 'impala://192.168.1.47:21050/%s' % game
    engine = create_engine(impala_url)
    connection = engine.raw_connection()
    print '''===RUNNING==='''
    df = pd.read_sql(sql, connection)
    print df
    return df
    connection.close()

if __name__ == '__main__':
    mission = 1
    while mission == 1:
        count = 1
        game_change = raw_input("game by 1:TW/2:PUB/3:TX:")
        if game_change == '1':
            game = 'dancer_tw'
        elif game_change == '2':
            game = 'dancer_pub'
        elif game_change == '3':
            game = 'dancer_tx'
        sql = raw_input("Enter Sql:")
        try:
            result = impala_sql(game,sql)
            output = raw_input("Output by 1:TXT/2:EXCEL/3:CSV:")
            if output == '1':
                f_name = raw_input("name is ?")
                result.to_csv('%s.txt' % f_name,index=False)
            elif output == '2':
                f_name = raw_input("name is ?")
                result.to_excel('%s.xlsx' % f_name,index=False)
            elif output == '3':
                f_name = raw_input("name is ?")
                result.to_csv('%s.csv' % f_name,index=False)
            else:
                pass
            press = '1'#raw_input("Press 1 to go on or Press else to quit:")
            if press == '1':
                mission = int(press)
            else:
                mission = 2
        except Exception, e:
            print e
print '''Thanks'''
time.sleep(1)
exit()
