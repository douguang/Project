#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Dong Junshuang
Description : 属性改造情况
'''
import settings
from utils import hqls_to_dfs, update_mysql, ds_add, hql_to_df
from pandas import Series,DataFrame
import pandas as pd

settings.set_env('superhero_vt')

sql = '''
SELECT uid,
       level
FROM raw_info
WHERE ds >= '20161016'
  AND ds <='20161022'
  AND regexp_replace(substr(create_time,1,10),'-','') = '20161016'
'''
tt_df = hql_to_df(sql)
