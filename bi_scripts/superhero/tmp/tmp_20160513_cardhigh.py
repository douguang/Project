#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Dong Junshuang
Description : 越南参与神域玩家卡牌培养情况 - 卡牌进阶状态
create date : 2016.05.13
'''
import settings
from utils import hql_to_df, update_mysql, ds_add
from pandas import Series,DataFrame
from utils import get_config
import pandas as pd

def commen_data(date):
# if __name__ == '__main__':
    settings.set_env('superhero_vt')
    print 'please wait a minuate'
    card_sql = '''
    select uid,card_id,jinjie from raw_card where is_fight=1 and  ds = '{0}'
    '''.format(date)
    card_df = hql_to_df(card_sql)
    print card_df

    god_area_sql ='''
    select uid,args from raw_action_log where action='god_field.set_god_field' and rc=0  and  ds = '{0}' order by uid
    '''.format(date)
    # and uid in ('vno30131436','vna10505800')
    god_area_df = hql_to_df(god_area_sql)
    print god_area_df

    have_card_sql = '''
    select uid,card_id,jinjie from raw_card
    left semi join (
    select * from raw_action_log where action='god_field.set_god_field' and rc=0 and  ds = '{0}'
    ) as a
    on raw_card.uid =  a.uid
    where ds = '{1}'
    '''.format(date,date)
    have_card_df = hql_to_df(have_card_sql)

    #取出神域中所有用到的卡牌 存入card_id
    m = 0
    uid = god_area_df['uid'][0]
    uid_id = []
    card_id = []
    for i in god_area_df['uid']:
        if i == uid:
            args_mes = god_area_df['args'][m]
            card_dict = eval(args_mes)
            card_list = card_dict['god_formation_1'][0].split('_')
            card_list2 = card_dict['god_formation_2'][0].split('_')
            card_list3 = card_dict['god_formation_3'][0].split('_')
            # print i
            # print card_list
            for k in card_list:
                if len(k)>2:
                    uid_id.append(i)
                    card_id.append(k.split('-')[0])
                else:
                    continue
            for k in card_list2:
                if len(k)>2:
                    uid_id.append(i)
                    card_id.append(k.split('-')[0])
                else:
                    continue
            for k in card_list3:
                if len(k)>2:
                    uid_id.append(i)
                    card_id.append(k.split('-')[0])
                else:
                    continue
            m = m+1
        else:
            uid = i
            args_mes = god_area_df['args'][m]
            card_dict = eval(args_mes)
            card_list = card_dict['god_formation_1'][0].split('_')
            card_list2 = card_dict['god_formation_2'][0].split('_')
            card_list3 = card_dict['god_formation_3'][0].split('_')
            # print i
            # print card_list
            for k in card_list:
                if len(k)>2:
                    uid_id.append(i)
                    card_id.append(k.split('-')[0])
                else:
                    continue
            for k in card_list2:
                if len(k)>2:
                    uid_id.append(i)
                    card_id.append(k.split('-')[0])
                else:
                    continue
            for k in card_list3:
                if len(k)>2:
                    uid_id.append(i)
                    card_id.append(k.split('-')[0])
                else:
                    continue
            m = m+1
    # print uid_id,card_id
    data = {'uid':uid_id,'card':card_id}
    frame = DataFrame(data)
    print frame
    #神域 中的uid（去重后的值）
    god_uid = frame.drop_duplicates(['uid'])

    frame['uid_card']=frame['uid']+frame['card']
    card_df['card_id']=card_df['card_id'].map(lambda s:str(s))
    card_df['uid_card']=card_df['uid']+card_df['card_id']
    frame['is_main_zhen'] = frame['uid_card'].isin(card_df.uid_card.values)
    #神域中除去主阵的信息
    god_card_df = frame[frame['is_main_zhen'] == False]
    #神域中除去主阵后每个用户对应的卡片信息
    god_card = god_card_df.drop_duplicates(['uid_card'])
    # 卡片及卡片上阵数量
    final_result = god_card.groupby('card').count().reset_index()

    # 所有卡片中是神域中的卡片的信息
    have_card_df['card_id'] = have_card_df['card_id'].map(lambda s:str(s))
    have_card_df['is_uid_card'] = have_card_df['uid']+have_card_df['card_id']
    have_card_df = have_card_df.drop_duplicates(['is_uid_card'])
    have_card_df['is_god_card'] = have_card_df['card_id'].isin(god_card.card.values)
    have_card_df = have_card_df[have_card_df['is_god_card'] == True]
    have_card_num = have_card_df.groupby('card_id').count().reset_index()
    final_result['fight_num'] = final_result['uid']
    have_card_num['have_num'] = have_card_num['uid']
    final_result['card_id'] = final_result['card']
    have_card_num['card'] = have_card_num['card_id']
    final_result =  final_result.merge(have_card_num,on=['card_id'],how='outer')


    jinjie0004_df = have_card_df[have_card_df['jinjie'] >= 0]
    jinjie0004_df = jinjie0004_df[jinjie0004_df['jinjie'] <= 4]
    jinjie0523_df = have_card_df[have_card_df['jinjie'] >= 5]
    jinjie0523_df = jinjie0523_df[jinjie0523_df['jinjie'] <= 23]
    jinjie2440_df = have_card_df[have_card_df['jinjie'] >= 24]
    jinjie2440_df = jinjie2440_df[jinjie2440_df['jinjie'] <= 40]
    jinjie4150_df = have_card_df[have_card_df['jinjie'] >= 41]
    jinjie4150_df = jinjie4150_df[jinjie4150_df['jinjie'] <= 50]
    jinjie5160_df = have_card_df[have_card_df['jinjie'] >= 51]
    jinjie5160_df = jinjie5160_df[jinjie5160_df['jinjie'] <= 60]
    jinjie0_df = have_card_df[have_card_df['jinjie'] == 0]
    # jinjie23_df = have_card_df[have_card_df['jinjie'] == 23]

    #进阶23-52
    for i in range(23,61):
        name = 'jinjie'+str(i)+'_df'
        name_num = 'jinjie'+str(i)+'_num'
        name = have_card_df[have_card_df['jinjie'] == i]
        name_num = name.groupby('card_id').count().reset_index()
        name_num['r%s_num' %i] = name_num['uid']
        final_result =  final_result.merge(name_num,on=['card_id'],how='outer')

    list_name = [[jinjie0004_df,'r0004_num'],[jinjie0523_df,'r0523_num'],[jinjie2440_df,'r2440_num'],[jinjie4150_df,'r4150_num'],[jinjie5160_df,'r5160_num']]
    for i,j in list_name:
        name_num = 'jinjie_'+j
        name = j
        name_num = i.groupby('card_id').count().reset_index()
        name_num['%s' %name] = name_num['uid']
        final_result =  final_result.merge(name_num,on=['card_id'],how='outer')

    jinjie0_num = jinjie0_df.groupby('card_id').count().reset_index()
    jinjie0_num['r0_num'] = jinjie0_num['uid']
    final_result =  final_result.merge(jinjie0_num,on=['card_id'],how='outer')

    final_result['use_rate'] = final_result['fight_num']/final_result['have_num']*100
    character_detail_cfg = get_config('character_detail')
    final_result['name'] = final_result['card_id'].map(lambda s: character_detail_cfg[s]['name'])
    final_result['ds'] = date
    columns = ['ds','card_id','name', 'have_num', 'fight_num', 'use_rate', 'r0004_num', 'r0523_num', 'r2440_num', 'r4150_num','r5160_num', 'r0_num', 'r23_num', 'r24_num','r25_num','r26_num','r27_num','r28_num','r29_num','r30_num','r31_num','r32_num','r33_num','r34_num','r35_num','r36_num','r37_num','r38_num','r39_num','r40_num','r41_num','r42_num','r43_num','r44_num','r45_num','r46_num','r47_num','r48_num','r49_num','r50_num','r51_num','r52_num','r53_num','r54_num','r55_num','r56_num','r57_num','r58_num','r59_num','r60_num']
    final_result = final_result[columns]

    uid_num = god_uid.count()['uid']
    user_data ={'uid_num':[uid_num],'ds':[date]}
    user_data_df = DataFrame(user_data)

    return [final_result,user_data_df]


if __name__ == '__main__':
    date = '20160427'
    print date
    final_result,user_num_result = commen_data(date)
    for i in range(16):
        if date<'20160512':
            date = ds_add(date,1)
            print date
            final_result_a,user_num_result_a = commen_data(date)
            final_result = pd.concat([final_result,final_result_a])
            user_num_result = pd.concat([user_num_result,user_num_result_a])
    final_result.to_excel('/Users/kaiqigu/Downloads/Excel/mm.xlsx')
    user_num_result.to_excel('/Users/kaiqigu/Downloads/Excel/kk.xlsx')
