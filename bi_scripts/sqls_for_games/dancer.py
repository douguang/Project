#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Hu Chunlong
Description : 武娘 通用sql语句
'''
import settings_dev
from utils import hql_to_df


def test(db):
    settings_dev.set_env(db)
    test_sql = '''
    SELECT *
    FROM raw_paylog
    WHERE ds = '20170320'
    LIMIT 10
    '''
    test_df = hql_to_df(test_sql)
    return test_df
