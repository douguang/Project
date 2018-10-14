#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Hu Wenchao
Description :
'''

from sqlalchemy.engine import create_engine

platform = 'sanguo_ks'
mysql_url = 'mysql://root:60aa954499f7ab@192.168.1.27/%s' % platform

engine = create_engine(mysql_url, echo=True)
with engine.connect() as con:
    rs = con.execute('show tables in sanguo')
    tables_to_copy = [i[0] for i in rs.fetchall() if 'vs_' in i[0]]
    print tables_to_copy
    for t in tables_to_copy:
        con.execute(
            'create table if not exists {platform}.{table} like sanguo.{table}'.format(
                **{
                    'table': t,
                    'platform': platform
                }))
