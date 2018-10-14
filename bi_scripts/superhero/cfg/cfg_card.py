#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Dong Junshuang
Description : 生成全部卡牌的id、名称
'''
from utils import update_mysql
import settings
from utils import get_config
from pandas import DataFrame


settings.set_env('superhero_bi')
plat = 'superhero_pub'
character_detail_config = get_config('character_detail')

card_id_list,card_name_list = [],[]
for card_id in character_detail_config.keys():
    card_name = character_detail_config[card_id].get('name')
    card_id_list.append(card_id)
    card_name_list.append(card_name)

result_df = DataFrame({'card_id':card_id_list,'card_name':card_name_list})
result_df['card_id'] = result_df['card_id'].map(lambda s: int(s))
print result_df

# 更新MySQL表
table = 'cfg_character_detail'
del_sql = 'delete from {0}'.format(table)
update_mysql(table, result_df, del_sql, plat)

