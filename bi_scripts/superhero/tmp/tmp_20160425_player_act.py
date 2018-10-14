#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Hu Wenchao
Description :
'''
import settings
from utils import hql_to_df
import pandas as pd

settings.set_env('superhero_bi')

hql = '''
select *
from superhero_bi.raw_action_log
where (ds = '20160417' or ds = '20160418')
      and uid = 'g72761334'
      and (action='item.use' or action='cards.card_evolution')
      and rc = '0'
'''

df = hql_to_df(hql)
print df
