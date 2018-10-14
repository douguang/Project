#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Dong Junshuang
Description : 根据输入条件，查询各版本的原数据
'''
import settings
from utils import hql_to_df, ds_add, date_range

platform_dic = {
1:'sanguo_ks'
,2:'sanguo_tx'
,3:'sanguo_ios'
,4:'sanguo_tw'
,5:'superhero_bi'
,6:'superhero_qiku'
,7:'superhero_tl'
,8:'superhero_vt'
,9:'superhero_self_en'
,10:'dancer_ks'
,11:'dancer_tw'
}

table_dic = {
1:'raw_paylog'
,2:'raw_spendlog'
,3:'raw_action_log'
}

def input_num(st):
    print 'please enter a %s number:' % st
    number = input()
    if st == 'version':
        if number not in range(1,len(platform_dic)+1):
            print 'Please re-enter a %s number' % st
            number = input()
        print 'you Has chosen version %s' % platform_dic[number]
    if st == 'table':
        if number not in range(1,len(platform_dic)+1):
            print 'Please re-enter a %s number' % st
            number = input()
        print 'you Has chosen table %s' % table_dic[number]

    return number

def export_file(df):
    # 是否导出Excel
    print 'Whether Export to Excel(y/n):'
    num = raw_input()
    if num == 'y' or num == 'Y':
        print 'Please enter Excel path and file name(example：c:/aa.xlsx):'
        path = raw_input()
        df.to_excel(path)

def raw_paylog_data(date,uid_list,plat):
    # 超级英雄国外
    sup_foren_sql = '''
    SELECT ds,
           uid,
           order_coin,
           order_time,
           order_money,
           order_rmb
    FROM raw_paylog
    WHERE ds = '{date}'
      AND uid in {uid_list}
    '''.format(date=date,uid_list = tuple(uid_list))
    # 超级英雄国内
    bi_sql = '''
    SELECT ds,
           uid,
           order_coin,
           order_time,
           order_money
    FROM raw_paylog
    WHERE ds = '{date}'
      AND uid in {uid_list}
    '''.format(date=date,uid_list = tuple(uid_list))
    # 三国、舞娘
    sql = '''
    SELECT ds,
           user_id,
           order_coin,
           order_time,
           order_money
    FROM raw_paylog
    WHERE ds = '{date}'
      AND user_id in {uid_list}
    '''.format(date=date,uid_list = tuple(uid_list))
    if plat in ['superhero_tl','superhero_vt','superhero_self_en']:
        sql_df = hql_to_df(sup_foren_sql)
    elif plat in ['superhero_bi','superhero_qiku']:
        sql_df = hql_to_df(bi_sql)
    else:
        sql_df = hql_to_df(sql)
    return sql_df

def raw_spendlog_data(date,uid_list,plat):
    # 超级英雄
    sup_sql = '''
    SELECT ds,
           uid,
           LEVEL,
           coin_num,
           goods_type,
           args
    FROM raw_spendlog
    WHERE ds = '{date}'
      AND uid in {uid_list}
    '''.format(date=date,uid_list = tuple(uid_list))
    # 三国、舞娘
    sql = '''
    SELECT ds,
           user_id,
           LEVEL,
           coin_num,
           goods_type,
           args
    FROM raw_spendlog
    WHERE ds = '{date}'
      AND user_id in {uid_list}
    '''.format(date=date,uid_list = tuple(uid_list))
    if plat in ['superhero_tl','superhero_vt','superhero_self_en','superhero_bi','superhero_qiku']:
        sql_df = hql_to_df(sup_sql)
    else:
        sql_df = hql_to_df(sql)
    return sql_df

def raw_action_data(date,uid_list,plat):
    # 超级英雄
    sup_sql = '''
    SELECT stmp,
           uid,
           action,
           rc,
           args
    FROM raw_action_log
    WHERE ds = '{date}'
      AND uid in {uid_list}
    '''.format(date=date,uid_list = tuple(uid_list))
    # 三国、舞娘
    sql = '''
    SELECT user_id,
           account,
           a_typ,
           a_tar
    FROM mid_actionlog
    WHERE ds = '{date}'
      AND user_id in {uid_list}
    '''.format(date=date,uid_list = tuple(uid_list))
    if plat in ['superhero_tl','superhero_vt','superhero_self_en','superhero_bi','superhero_qiku']:
        sql_df = hql_to_df(sup_sql)
    else:
        sql_df = hql_to_df(sql)
    return sql_df

if __name__ == '__main__':
    print '-'*40
    print 'This is all games version.'
    for i in platform_dic.keys():
        print i,'------',platform_dic[i]
    print '-'*40
    number = input_num('version')
    settings.set_env(platform_dic[number])
    plat = platform_dic[number]

    print '-'*40
    print 'This is all table.'
    for i in table_dic.keys():
        print i,'------',table_dic[i]
    print '-'*40
    number = input_num('table')

    if number == 1:
        print 'Please enter the date (format: yyyymmdd)'
        date = raw_input()
        uid_list = ['']
        uid = ''
        while uid != 'q':
            print 'Please enter uid(or enter q end input):'
            uid = raw_input()
            uid_list.append(uid)
        print 'Please wait a minute'
        df = raw_paylog_data(date,uid_list,plat)
        print df
        export_file(df)
    if number == 2:
        print 'Please enter the date (format: yyyymmdd)'
        date = raw_input()
        uid_list = ['']
        uid = ''
        while uid != 'q':
            print 'Please enter uid(or enter q end input):'
            uid = raw_input()
            uid_list.append(uid)
        print 'Please wait a minute'
        df = raw_spendlog_data(date,uid_list,plat)
        print df
        export_file(df)
    if number == 3:
        print 'Please enter the date (format: yyyymmdd)'
        date = raw_input()
        uid_list = ['']
        uid = ''
        while uid != 'q':
            print 'Please enter uid(or enter q end input):'
            uid = raw_input()
            uid_list.append(uid)
        print 'Please wait a minute'
        df = raw_action_data(date,uid_list,plat)
        print df
        export_file(df)

