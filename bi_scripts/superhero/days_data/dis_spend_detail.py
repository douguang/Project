#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Dong Junshuang
Description : 消费详情(批量补数据)
注：需手动删除更新的数据
'''
from utils import hqls_to_dfs, update_mysql
import settings

settings.set_env('superhero_bi')
plat = 'superhero_ios'
start_date = '20160501'
end_date = '20160825'

spend_sql = '''
SELECT ds,
       uid,
       reverse(substr(reverse(uid), 8)) AS server,
       goods_type api,
       coin_num spend_num
FROM raw_spendlog
WHERE substr(uid,1,1) = 'a'
and ds >= '{0}'
  AND ds <= '{1}'
'''.format(start_date,end_date)
info_sql = '''
SELECT ds,
       uid,
       vip_level
FROM raw_info
WHERE substr(uid,1,1) = 'a'
and ds >= '{0}'
  AND ds <= '{1}'
'''.format(start_date,end_date)
spend_df, info_df = hqls_to_dfs([spend_sql, info_sql])
result_data = spend_df.merge(info_df, on=['ds','uid'], how='left')
result_df = (result_data.groupby(['ds','server', 'vip_level', 'api']).sum(
).reset_index().loc[:, ['ds','server', 'vip_level', 'api', 'spend_num']])

print result_df

# 更新消费详情的MySQL表
table = 'dis_spend_detail'
del_sql = 'delete from {0} where ds="{1}"'.format(table, end_date)
update_mysql(table, result_df, del_sql, plat)




