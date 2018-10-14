#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Dong Junshuang
Description : 世界之树
'''
from utils import hql_to_df, update_mysql
import settings
from pandas import DataFrame
from get_equip_info import get_equip_df

if __name__ == '__main__':
    settings.set_env('superhero_qiku')
    sql ='''
    SELECT uid,
           args
    FROM raw_action_log
    WHERE action='world_tree.buy'
      AND ds >= '20160701'
      AND ds <= '20160704'
    '''
    word_df = hql_to_df(sql)
    uid_list = ['q734568713',
'q716137385',
'q163319144',
'q536655564',
'q172969719',
'q27672777',
'q711221831',
'q738884345',
'q669208005',
'q739164314',
'q723806722',
'q669524370',
'q390947338',
'q584072723',
'q723665583',
'q500710639',
'q745568975',
'q720630706',
'q25523543',
'q692797601',
'q719944233',
'q748023588',
'q661716542']

word_df['is_uid'] = word_df['uid'].isin(uid_list)
result = word_df[word_df['is_uid']]
del result['is_uid']

uid_list,mm_list = [],[]
for i in range(len(result)):
    uid = result.iloc[i,0]
    args = result.iloc[i,1]
    args = eval(args)
    if args.has_key('goods_id'):
        # print args['goods_id']
        print args['goods_id'][0]
        uid_list.append(uid)
        mm_list.append(args['goods_id'][0])
    else:
        continue
data = DataFrame({'uid':uid_list,'goods_id':mm_list})
data['num'] =1
result_df = data.groupby(['uid','goods_id']).count().reset_index()
result_df = result_df.sort_values(by=['uid','goods_id'])
result_df.to_excel('/Users/kaiqigu/Downloads/Excel/word.xlsx')





