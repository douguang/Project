#!/usr/bin/env python  
# encoding: utf-8  

""" 
@author: Andy 
@site:  
@software: PyCharm 
@file: wumeng_data.py 
@time: 18/2/1 下午4:54 
"""



import calendar
import settings_dev
from utils import ds_add, hql_to_df, update_mysql, hqls_to_dfs, date_range
import datetime
import pandas as pd
import sys
reload(sys)
sys.setdefaultencoding('utf8')


def data_reduce():
    settings_dev.set_env('dancer_cgame')

    info_cgame_sql = '''
      select ds,ass_id,player  from raw_association where ds='20180201' group by ds,ass_id, player order by ds desc ,ass_id, player
    '''
    print info_cgame_sql
    info_cgame_df = hql_to_df(info_cgame_sql)
    print info_cgame_df.head()

    info_sql = '''
        select user_id,account,combat,reverse(substring(reverse(user_id), 8)) AS server from mid_info_all where ds='20180201' group by user_id,account,combat,server
    '''
    print info_sql
    info_df = hql_to_df(info_sql)
    print info_df.head()
    info_account_dict = {a: b for a, b in info_df[['user_id','account',]].get_values()}
    info_combat_dict = {a: b for a, b in info_df[['user_id','combat',]].get_values()}
    info_server_dict = {a: b for a, b in info_df[['user_id','server',]].get_values()}
    print info_account_dict

    def card_evo_lines():
        for _, row in info_cgame_df.iterrows():
            for user_id in eval(row.player):
                account = info_account_dict.get(user_id, '')
                combat = info_combat_dict.get(user_id, '')
                server = info_server_dict.get(user_id, '')
                print [row.ds,row.ass_id,user_id,account,combat,server,]
                yield [row.ds,row.ass_id,user_id,account,combat,server,]

    result_df = pd.DataFrame(card_evo_lines(), columns=['ds', 'ass_id', 'user_id','account','combat','server',])
    print result_df.head()
    result_df['rank'] = result_df['combat'].groupby(result_df['ass_id']).rank(ascending=False)

    result_df.to_excel(r'/Users/kaiqigu/Documents/Dancer/武娘-cgame-武盟数据_20180203.xlsx', index=False)

if __name__ == '__main__':
    result = data_reduce()

    print "end"