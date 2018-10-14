#!/usr/bin/env python  
# encoding: utf-8  

""" 
@author: Andy 
@site:  合服战力排名前10
@software: PyCharm 
@file: superhero_pub_hefu_zhanlipaiming.py 
@time: 17/9/7 下午4:01 
"""

import settings_dev
from utils import ds_add, hql_to_df
import pandas as pd

def get_ser_data():
    '''
    将[主、从服、从服..]数据：'vno4', 'vnq6', 'vnt0', 'vnu0'],
    生成为[主服\t从服]数据
    并返回[主服、从服]数据的DataFrame
    '''
    path = r'/Users/kaiqigu/Documents/Superhero/'
    file_in = path + 'superhero_pub_hefu.txt'
    file_out = path + 'superhero_pub_res.txt'
    f_in = open(file_in,'r')
    f_out = open(file_out,'w')
    f_out.write(str('father_ser') + '\t' + str('son_ser') + '\n')
    for l_raw in f_in:
        try:
            for i in tuple(eval(l_raw))[0]:
                f_out.write(str(tuple(eval(l_raw))[0][0]) + '\t' + str(i) + '\n')
        except:
            print 'error !!!'
    f_in.close()
    f_out.close()
    print 'ser date complete'
    ser_df = pd.read_table(file_out,sep='\t')
    ser_df = ser_df.rename(columns={'son_ser': 'server_id', })
    print ser_df

    server_info_sql = '''
        select t1.server_id,t1.combat,t1.rank,t1.uid from (
            select reverse(substring(reverse(uid),8)) as server_id,max(zhandouli) as combat,
                       row_number() over( partition by  reverse(substring(reverse(uid),8))  order by max(zhandouli)  DESC ) as rank,
                       uid
            from mid_info_all where ds='20170906' and fresh_time >='2017-09-01 00:00:00'
            group by server_id,uid
        )t1
        where t1.rank<=10 and t1.server_id != '' and t1.server_id like 'g%'
    '''
    server_info_df = hql_to_df(server_info_sql)
    print server_info_df.head()

    result_df = server_info_df.merge(ser_df, on=['server_id',], how='left')
    result_df['rank'] = result_df['combat'].groupby(result_df['father_ser']).rank(ascending=False)
    # result_df['rank'] = result_df['father_ser','spend',].rank(ascending=False).astype("int")
    print result_df.head()

    return result_df

if __name__ == '__main__':
    settings_dev.set_env('superhero_bi')
    plat = 'g'
    res = get_ser_data()
    print res
    res.to_excel('/Users/kaiqigu/Documents/Sanguo/超级英雄-国内-合服战力排名_20170907-2.xlsx')
    # 获取每个子服的数据