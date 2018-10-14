#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Dong Junshuang
Description : 越南参与神域玩家卡牌培养情况--转生
Create date : 2016.05.16
'''
import settings
from utils import hql_to_df, update_mysql, ds_add
from pandas import Series,DataFrame
from utils import get_config
import pandas as pd

def get_card_info(date,god_area_sql):
    god_area_df = hql_to_df(god_area_sql)
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
    data = {'uid':uid_id,'card':card_id}
    card_data = DataFrame(data)
    card_data['ds'] = date
    return card_data

def god_area_info(date):
    #当天设置过神域阵型的数据（用户、卡片）
    god_area_sql ="select uid,args from raw_action_log where action='god_field.set_god_field' and rc=0  and  ds = '{0}' order by uid".format(date)
    print god_area_sql
    god_area_df = get_card_info(date,god_area_sql)
    # god_area_df['card_id'] = god_area_df['card_id'].map(lambda s:str(s))
    god_area_df['uid_card']=god_area_df['uid']+god_area_df['card']
    god_area_df = god_area_df.drop_duplicates(['uid_card'])
    return god_area_df

def final_set_field_info(date):
    #当天最终阵型
    final_set_field_sql = '''
    select uid,max(args) as args from raw_action_log where action='god_field.set_god_field' and rc=0 and  ds = '{date}'  group by uid having count(uid)=1
    union all
    select uid,args from raw_action_log a
    left semi join
    (
    select uid,max(act_time) as ac_time from raw_action_log where action='god_field.set_god_field' and rc=0 and  ds = '{date}'  group by uid having count(uid)>1
    ) b on a.uid=b.uid and a.act_time = b.ac_time and action='god_field.set_god_field' and rc=0 and a.ds = '{date}'
    '''.format(**{
        'date': date
        })
    print final_set_field_sql
    final_set_field_df = get_card_info(date,final_set_field_sql)
    return final_set_field_df

def mid_data_info(date,final_set_field_df):
    today_final_field_df = final_set_field_info(date)
    today_final_field_df['is_today_uid'] = today_final_field_df['uid'].isin(final_set_field_df.uid.values)
    final_set_field_df['is_today_uid'] = final_set_field_df['uid'].isin(today_final_field_df.uid.values)
    today_data = today_final_field_df[today_final_field_df['is_today_uid']==True]
    yes_data = final_set_field_df[final_set_field_df['is_today_uid'] == False]
    mid_data = pd.concat([today_data,yes_data])
    mid_data['ds'] = date
    return mid_data

def is_main_card(date):
    main_card_sql = '''
    select uid,card_id from raw_card where is_fight=1 and  ds = '{0}'
    '''.format(date)
    main_card_df = hql_to_df(main_card_sql)
    main_card_df['card_id'] = main_card_df['card_id'].map(lambda s:str(s))
    main_card_df['uid_card'] = main_card_df['uid'] + main_card_df['card_id']
    main_card_df = main_card_df.drop_duplicates(['uid_card'])
    print main_card_sql
    return main_card_df

def is_battle_info(date):
    battle_uid_sql = '''
    select uid,card_id,zhuansheng from raw_card
    left semi join (
    select uid from raw_action_log where action='god_field.god_field_battle' and  ds = '{0}'
    ) as a
    on raw_card.uid =  a.uid
    where ds = '{1}'
    '''.format(date,date)
    print battle_uid_sql
    battle_uid_df = hql_to_df(battle_uid_sql)
    battle_uid_df['card_id'] = battle_uid_df['card_id'].map(lambda s:str(s))
    battle_uid_df['uid_card'] = battle_uid_df['uid'] + battle_uid_df['card_id']
    battle_uid_df = battle_uid_df.drop_duplicates(['uid_card'])
    return battle_uid_df

def zhuansehng_info(final_result,date):
    for i in range(0,8):
        df_name = final_result[final_result['zhuansheng']== i]
        num_name = df_name.groupby('card_id').count().reset_index()
        num_name['r%s_num' %i] = num_name['uid']
        columns = ['card_id','r%s_num' %i]
        num_name = num_name[columns]
        print num_name
        if i == 0:
            zhuansheng_num = num_name
        else:
            zhuansheng_num =  zhuansheng_num.merge(num_name,on=['card_id'],how='outer')
    zhuansheng_num['ds'] = date
    return zhuansheng_num

def final_result_data_info(final_set_field_df,date):
    mid_data = mid_data_info(date,final_set_field_df)
    # 当天设置过神域阵型的数据（用户、卡片）
    god_area_df = god_area_info(date)
    # 参加战斗的用户拥有卡片的信息
    battle_uid_df = is_battle_info(date)
    # 主阵中卡片信息
    main_card_df = is_main_card(date)
    #参加战斗且不是主阵的信息
    battle_uid_df['is_main_card'] = battle_uid_df['uid_card'].isin(main_card_df.uid_card.values)
    battle_uid_df['is_god_card'] = battle_uid_df['uid_card'].isin(god_area_df.uid_card.values)
    battle_uid_df['is_god_uid'] = battle_uid_df['uid'].isin(god_area_df.uid.values)
    # 参加战斗且非主阵的信息
    card_result = battle_uid_df[battle_uid_df['is_main_card'] == False]
    #参见战斗且非主阵 且设置过阵型的信息
    set_field_result = card_result[card_result['is_god_card'] == True]
    columns = ['uid','card_id','uid_card']
    set_field_result = set_field_result[columns]
    #参见战斗且非主阵 且没有设置过阵型的信息
    mid_data['is_battle_uid'] = mid_data['uid'].isin(god_area_df.uid.values)
    result = mid_data[mid_data['is_battle_uid'] == False]
    result['card'] = result['card'].map(lambda s:str(s))
    result['uid_card'] = result['uid'] + result['card']
    result = result.drop_duplicates(['uid_card'])
    result['is_main_card'] = result['uid_card'].isin(main_card_df.uid_card.values)
    result = result[result['is_main_card'] == False]
    result['card_id'] = result['card']
    columns = ['uid','card_id','uid_card']
    set_field_result = set_field_result[columns]
    result = result[columns]
    result_data = pd.concat([set_field_result,result])
    battle_uid_df['is_use_card'] = battle_uid_df['uid_card'].isin(result_data.uid_card.values)
    result_data = battle_uid_df[battle_uid_df['is_use_card']==True]
    result_data = result_data.drop_duplicates(['uid_card'])
    # final_result_data = zhuansehng_info(result_data,date)

    # 上阵人数
    fight_num = result_data.groupby('card_id').count().reset_index()
    fight_num['fight_num'] = fight_num['uid']
    columns= ['card_id','fight_num']
    fight_num = fight_num[columns]
    # 拥有人数
    have_num = battle_uid_df.groupby('card_id').count().reset_index()
    have_num['have_num'] = have_num['uid']
    columns= ['card_id','have_num']
    have_num = have_num[columns]
    final_result_data = zhuansehng_info(result_data,date)
    final_result_data = (final_result_data
                        .merge(have_num,on=['card_id'])
                        .merge(fight_num,on=['card_id'])
                        )



    return final_result_data

if __name__ == '__main__':
    settings.set_env('superhero_vt')
    print 'please wait a minuate'
    date = '20160427'
    #当天设置过神域阵型的数据（用户、卡片）  玩家：293
    god_area_df = god_area_info(date)
    #参加战斗的用户拥有卡片的信息           玩家：160
    battle_uid_df = is_battle_info(date)
    #主阵中卡片信息                       玩家：32426
    main_card_df = is_main_card(date)
    # 当天最终阵型的数据
    final_set_field_df = final_set_field_info(date)
    #参加战斗且不是主阵的信息
    battle_uid_df['is_main_card'] = battle_uid_df['uid_card'].isin(main_card_df.uid_card.values)
    battle_uid_df['is_god_card'] = battle_uid_df['uid_card'].isin(god_area_df.uid_card.values)
    battle_uid_df = battle_uid_df.drop_duplicates(['uid_card'])
    final_result = battle_uid_df[battle_uid_df['is_main_card'] == False]
    final_result = final_result[final_result['is_god_card'] == True]
    # 上阵人数
    fight_num = final_result.groupby('card_id').count().reset_index()
    fight_num['fight_num'] = fight_num['uid']
    columns= ['card_id','fight_num']
    fight_num = fight_num[columns]
    # 拥有人数
    have_num = battle_uid_df.groupby('card_id').count().reset_index()
    have_num['have_num'] = have_num['uid']
    columns= ['card_id','have_num']
    have_num = have_num[columns]
    final_result = zhuansehng_info(final_result,date)
    final_result = (final_result
                    .merge(have_num,on=['card_id'])
                    .merge(fight_num,on=['card_id'])
                    )

    for i in range(16):
    # for i in range(1):
        if date<'20160512':
            date = ds_add(date,1)
            print date
            final_result_data = final_result_data_info(final_set_field_df,date)
            final_result = pd.concat([final_result,final_result_data])

    character_detail_cfg = get_config('character_detail')
    final_result['name'] = final_result['card_id'].map(lambda s: character_detail_cfg[s]['name'])
    columns = ['ds','card_id','name','have_num','fight_num','r0_num','r1_num','r2_num', 'r3_num','r4_num','r5_num','r6_num','r7_num']
    final_result = final_result[columns]
    final_result.to_excel('/Users/kaiqigu/Downloads/Excel/zhuangsheng.xlsx')






















