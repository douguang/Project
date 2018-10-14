#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Dong Junshuang
Description : 限时兑换数据
Time        : 2017.06.19
illustration: 限时兑换ID 限时兑换次数
限时兑换老服：omni_exchange.omni_exchange
限时兑换新服：server_exchange.server_omni_exchange
限时超得领取礼包：limit_active.get_charge_reward
'''
import pandas as pd
import settings_dev
from utils import hql_to_df

# 查询的ID
id_list = [4679, 4680, 4681, 4682, 4683, 4684, 4685, 4686, 4687, 4688]

settings_dev.set_env('dancer_pub')
sql = '''
SELECT ds,
       user_id,
       server,
       a_typ,
       a_tar args
FROM parse_actionlog
WHERE ds >='20170707'
and ds <= '20170711'
-- and a_typ = 'limit_active.get_charge_reward'
and a_typ in ('omni_exchange.omni_exchange'
,'server_exchange.server_omni_exchange')
  -- AND a_typ = 'server_exchange.server_omni_exchange'
'''
tt_df = hql_to_df(sql)


def get_id():
    for _, row in tt_df.iterrows():
        yield [row.user_id, eval(row.args)['id']]
        # yield [row.user_id, eval(row.args)['active_id']]


column = ['user_id', 'id']
result_df = pd.DataFrame(get_id(),
                         columns=column).groupby('id').count().reset_index()
result_df['id'] = result_df.astype('int')
result_df = result_df[result_df.id.isin(id_list)]
result_df.to_excel('/Users/kaiqigu/Documents/Excel/omni_exchange.xlsx')
# result_df.to_excel('/Users/kaiqigu/Documents/Excel/charge_reward.xlsx')
