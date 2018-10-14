#!/usr/bin/env python  
# encoding: utf-8  

""" 
@author: Andy 
@site:  资源变化
@software: PyCharm 
@file: ziyuanbianhua.py 
@time: 17/9/12 下午6:43 
"""

from utils import hqls_to_dfs, get_rank, hql_to_df, date_range
import pandas as pd
from utils import ds_delta
import settings_dev
from pandas import DataFrame

def data_reduce(date,date_reborn):
    info_sql = '''
    select ds,user_id,a_typ,a_tar,log_t from parse_action_log where ds='{date}'and a_typ like '%private_city.battle_end%' and user_id in (select user_id from mid_info_all where ds='{date}'  and regexp_replace(substring(reg_time,1,10),'-','')='{date_reborn}' ) group by ds,user_id,a_typ,a_tar,log_t
    '''.format(date=date,date_reborn=date_reborn)
    print info_sql
    info_df = hql_to_df(info_sql)
    print info_df.head()

    def card_evo_lines():
        for _, row in info_df.iterrows():
            # print row.a_tar
            try:
                win = eval(row.a_tar)['win'][0]
                if win == '1':
                    chapter_id = eval(row.a_tar)['chapter_id'][0]
                    stage_id = eval(row.a_tar)['stage_id'][0]
                    stage_team = eval(row.a_tar)['stage_team'][0]
                    degree = eval(row.a_tar)['degree'][0]
                    print [row.ds, row.user_id, row.a_typ, win,chapter_id,stage_id,stage_team,degree]
                    yield [row.ds, row.user_id, row.a_typ, win,chapter_id,stage_id,stage_team,degree]
            except:
                pass

    info_df = pd.DataFrame(card_evo_lines(), columns=[
        'ds', 'user_id', 'a_typ', 'win','chapter_id','stage_id','stage_team','degree',])
    print info_df.head()

    result_df = info_df.sort_values(by=['ds','user_id', 'a_typ','chapter_id','stage_id'], ascending=False).groupby(['ds','user_id',], as_index=False).first()
    print result_df.head()
    result_df = result_df.groupby(['ds', 'a_typ','chapter_id','stage_id',]).agg(
        {'user_id': lambda g: g.nunique(),}).reset_index()
    result_df = result_df.rename(columns={'user_id': 'user_id_num'})
    print result_df.head()

    return result_df


if __name__ == '__main__':
    settings_dev.set_env('superhero2_tw')
    result_list = []
    date_reborn = '20170910'
    for date in date_range('20170910','20170911'):
        res = data_reduce(date,date_reborn)
        result_list.append(res)
    pd.concat(result_list).to_excel(u'/Users/kaiqigu/Documents/Superhero/超级英雄2-台湾-10日所有玩家在10-11日-副本通关数据_20170912.xlsx')
    print 'end'