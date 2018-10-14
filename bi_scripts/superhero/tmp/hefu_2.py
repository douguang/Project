#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Dong Junshuang
Description : 重写合服数据脚本
date        : 2016-12-20
注：
跑脚本前需修改：date、plat、path、file_in、file_out
主服指合服之后的服务器，对应father_ser；
从服指合服之前的服务器，对应son_ser；
玩家合服之后uid保持不变，解析uid得到的服务器为从服son_ser。
主服  近7天平均DAU    近7天充值总人数    近7天充值总金额    最大战斗力   前十平均战斗力
'''
import settings_dev
from utils import ds_add, hql_to_df
import pandas as pd

def get_ser_data():
    '''
    将[主、从服、从服..]数据：'vno4', 'vnq6', 'vnt0', 'vnu0'],
    生成为[主服\t从服]数据
    并返回[主服、从服]数据的DataFrame
    '''
    path = r'/Users/kaiqigu/Documents/Excel/'
    file_in = path +  'qiku_ser_list.txt'
    file_out = path + 'qiku_ser_result.txt'
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
    return ser_df

if __name__ == '__main__':
    settings_dev.set_env('superhero_qiku')
    date = '20170410'
    plat = 'q'
    # 获取每个子服的数据
    info_sql = '''
    SELECT a.ds,
           reverse(substring(reverse(a.uid), 8)) AS son_ser,
           a.uid,
           a.zhandouli,
           nvl(b.order_rmb,0) order_rmb
    FROM
      (SELECT ds,
              uid,
              zhandouli
       FROM raw_info
       WHERE substr(uid,1,1) = '{plat}'
         AND ds >= '{date_ago}'
         AND ds <= '{date}')a
    LEFT OUTER JOIN
      (SELECT ds,
              uid,
              sum(order_money) order_rmb
              -- sum(order_rmb) order_rmb
       FROM raw_paylog
       WHERE substr(uid,1,1) = '{plat}'
         AND ds >= '{date_ago}'
         AND ds <= '{date}'
         AND platform_2 <> 'admin_test'
       GROUP BY ds,
                uid )b ON a.ds = b.ds
    AND a.uid = b.uid
    '''.format(date_ago=ds_add(date, -6), date=date, plat=plat)

    info_df = hql_to_df(info_sql)

    # 获取主服、从服数据
    ser_df = get_ser_data()

    info_ser = info_df.merge(ser_df,on='son_ser')

    # 近7天平均dau
    info_result_df = info_ser.groupby('father_ser').count().uid.reset_index()
    info_result_df['avg_dau'] = info_result_df['uid']*1.0/7
    del info_result_df['uid']
    # 近7天充值总人数
    pay_df = info_ser[info_ser.order_rmb > 0]
    pay_num_result_df = (pay_df.drop_duplicates(['father_ser','uid'])
                    .groupby('father_ser')
                    .count()
                    .uid
                    .reset_index()
                    .rename(columns={'uid':'pay_num'}))
    # 近7天充值总金额
    pay_money_result_df = (pay_df
                    .groupby('father_ser')
                    .sum()
                    .order_rmb
                    .reset_index()
                    .rename(columns={'order_rmb':'pay_money'}))
    # 最大战斗力
    max_battle_df = (info_ser
                    .groupby('father_ser')
                    .max()
                    .zhandouli
                    .reset_index()
                    .rename(columns={'zhandouli':'max_battle'}))
    # 前十平均战斗力
    battle_df = (info_ser
                .groupby(['father_ser','uid'])
                .max()
                .zhandouli
                .reset_index()
                )
    dfs = []
    for father_ser in battle_df.drop_duplicates('father_ser')['father_ser'].tolist():
        ser_battle_df = battle_df[battle_df.father_ser == father_ser]
        ser_battle_df = ser_battle_df.sort_values(by='zhandouli',ascending=False)
        ser_battle_df['zhandouli'] = ser_battle_df['zhandouli'].astype('float')
        battle_data =  (ser_battle_df[0:10]
                        .groupby('father_ser')
                        .mean()
                        .reset_index()
                        .rename(columns={'zhandouli':'avg_battle'})
                        )
        dfs.append(battle_data)
    battle_result = pd.concat(dfs)

    result_df = (info_result_df.merge(pay_num_result_df,on='father_ser')
        .merge(pay_money_result_df,on='father_ser')
        .merge(max_battle_df,on='father_ser')
        .merge(battle_result,on='father_ser')
        )

    columns = ['father_ser','avg_dau','pay_num','pay_money','max_battle','avg_battle']
    result_df = result_df[columns]

    result_df.to_excel('/Users/kaiqigu/Documents/Excel/hefu_data_{plat}_{date}.xlsx'.format(plat=plat,date=date))

# =======================下面为未合服的数据==========================

    info_ser = info_df.merge(ser_df,on='son_ser',how='left')
    info_son_ser = info_ser[info_ser.father_ser.isnull().values == True]

    if len(info_son_ser) > 0:
        # 近7天平均dau
        info_result_df = info_son_ser.groupby('son_ser').count().uid.reset_index()
        info_result_df['avg_dau'] = info_result_df['uid']*1.0/7
        del info_result_df['uid']
        # 近7天充值总人数
        pay_df = info_son_ser[info_son_ser.order_rmb > 0]
        pay_num_result_df = (pay_df.drop_duplicates(['son_ser','uid'])
                        .groupby('son_ser')
                        .count()
                        .uid
                        .reset_index()
                        .rename(columns={'uid':'pay_num'}))
        # 近7天充值总金额
        pay_money_result_df = (pay_df
                        .groupby('son_ser')
                        .sum()
                        .order_rmb
                        .reset_index()
                        .rename(columns={'order_rmb':'pay_money'}))
        # 最大战斗力
        max_battle_df = (info_son_ser
                        .groupby('son_ser')
                        .max()
                        .zhandouli
                        .reset_index()
                        .rename(columns={'zhandouli':'max_battle'}))
        # 前十平均战斗力
        battle_df = (info_son_ser
                    .groupby(['son_ser','uid'])
                    .max()
                    .zhandouli
                    .reset_index()
                    )
        dfs = []
        for son_ser in battle_df.drop_duplicates('son_ser')['son_ser'].tolist():
            ser_battle_df = battle_df[battle_df.son_ser == son_ser]
            ser_battle_df = ser_battle_df.sort_values(by='zhandouli',ascending=False)
            ser_battle_df['zhandouli'] = ser_battle_df['zhandouli'].astype('float')
            battle_data =  (ser_battle_df[0:10]
                            .groupby('son_ser')
                            .mean()
                            .reset_index()
                            .rename(columns={'zhandouli':'avg_battle'})
                            )
            dfs.append(battle_data)
        battle_result = pd.concat(dfs)

        result_df = (info_result_df.merge(pay_num_result_df,on='son_ser')
            .merge(pay_money_result_df,on='son_ser')
            .merge(max_battle_df,on='son_ser')
            .merge(battle_result,on='son_ser')
            )

        columns = ['son_ser','avg_dau','pay_num','pay_money','max_battle','avg_battle']
        result_df = result_df[columns]

        result_df.to_excel('/Users/kaiqigu/Documents/Excel/hefu_data_son_{plat}_{date}.xlsx'.format(plat=plat,date=date))
    else:
        print 'not have son_ser'
