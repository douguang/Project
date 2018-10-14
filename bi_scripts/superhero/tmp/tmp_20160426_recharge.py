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
select uid, args
from raw_action_log
where ds >= '20160418'
      and ds <= '20160424'
      and action = 'active.active_recharge_receive'
      and rc = '0'
'''

df = hql_to_df(hql)
print df
df['active_id'] = df.args.map(lambda s: eval(s)['active_id'][0])
result = df.groupby('active_id').count().reset_index()
result.to_excel('/tmp/rechage_info.xlsx')
