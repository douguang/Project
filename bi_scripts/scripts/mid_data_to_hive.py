#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Zhang Yongchen
Description : 执行sql补mid_new_account数据
Time        : 2017.03.23
'''
import os
import settings_dev
import sqlalchemy
from settings_dev import hive_path
from lib.utils import load_data_to_file
from lib.utils import ds_add
from lib.utils import ColorPrint
from utils import date_range, ds_add

if __name__ == '__main__':
    platform = 'dancer_pub'
    task_name = 'mid_new_account'
    settings_dev.set_env(platform)

    for date_str in date_range('20161110', '20170606'):
        message = '>>>start run {0} {1} {2}'.format(platform, task_name, date_str)
        ColorPrint(message)
        mid_new_account_sql = '''
            INSERT overwrite TABLE mid_new_account partition (ds='{date_str}')
            SELECT a.account, a.platform FROM
            (SELECT
                   distinct account,
                   substr(account,1,instr(account,'_')-1) as platform
            FROM   parse_info
            WHERE ds= '{date_str}') a
            left outer join
            (SELECT account
            FROM mid_info_all
            WHERE ds= '{yestoday}') b
            on a.account=b.account where b.account is null
        '''.format(**{'date_str': date_str, 'yestoday': ds_add(date_str, -1)})

        hive_template = 'hive://192.168.1.8:10000/{db}'
        url = hive_template.format(db=platform)
        engine = sqlalchemy.create_engine(url)
        conn = engine.raw_connection()
        cur = conn.cursor()
        cur.execute(mid_new_account_sql)
        conn.close()
