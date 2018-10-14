#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Hu Chunlong
Description : 注册用户LTV
'''
from utils import hql_to_df
import settings_dev
from pandas import DataFrame

settings_dev.set_env('dancer_tw')
ass_sql = '''
select ass_id,player
from raw_association
where ds >= '20160922'
'''
ass_df = hql_to_df(ass_sql)
player_df = ass_df['player'].copy()
ass_id, player = [], []
for i in range(len(ass_df)):
    print i
    # a_tar = ass_df.iloc[i,1]
    # a_tar = eval(a_tar)
    # ass_id.append(ass_df.iloc[i , 0])
    # player.append(ass_df.iloc[i , 1])
ass_list = DataFrame({'ass_id':ass_id, 'player':player})
print ass_list