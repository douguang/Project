#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Hu Wenchao
Description :
'''
import settings
from utils import hql_to_df
import pandas as pd

settings.set_env('superhero_vt')

hql = '''
select uid, args
from superhero_vt.raw_action_log
where (ds = '20160424' or ds = '20160423') and action = 'active.active_consume_receive'
'''

df = hql_to_df(hql)
print df
