#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Zhang Yongchen
Description : 20170117日    active  活动副本 就是峨嵋宗师 逍遥宗师 逍遥坊 练功房 宗派高手 五个活动的每日人均次数
Database    : dancer_pub
'''
import settings_dev
import pandas as pd
from utils import hql_to_df, date_range
def tmp_20170117_active(date):

    #活动副本
    active_sql = '''
        select user_id, ds, a_tar from parse_actionlog where ds='{date}' and a_typ='active.fight'
    '''.format(date=date)
    print active_sql
    active_df = hql_to_df(active_sql)
    print active_df.head(10)

    user_id_list, ds_list, chapter_list, active_id_list = [], [], [], []
    for i in range(len(active_df)):
        user_id_list.append(active_df.iloc[i,0])
        ds_list.append(active_df.iloc[i, 1])
        tar = active_df.iloc[i, 2]
        tar = eval(tar)
        chapter = 0
        # active_id = 0
        try:
            chapter = tar['chapter']
            # active_id = tar['active_id']
        except:
            pass
        chapter_list.append(chapter)
        # active_id_list.append(active_id)
        data = pd.DataFrame({'user_id': user_id_list, 'ds': ds_list, 'chapter': chapter_list})
    print data.head(10)

    data['num'] = 1
    data = data.groupby(['ds', 'chapter']).agg({
        'user_id': lambda g: g.nunique(),
        'num': lambda g: g.count()
    }).reset_index()
    print data.head(10)

    # 购买次数
    spend_sql = '''
        select user_id, ds, args from raw_spendlog where ds='{date}' and goods_type='active.buy_fight_times'
    '''.format(date=date)
    spend_df = hql_to_df(spend_sql)
    user_id_list, ds_list, chapter_list, active_id_list = [], [], [], []
    for i in range(len(spend_df)):
        user_id_list.append(spend_df.iloc[i, 0])
        ds_list.append(spend_df.iloc[i, 1])
        args = spend_df.iloc[i, 2]
        args = eval(args)
        # print args
        chapter = 0
        try:
            chapter = args['chapter'][0]
        except:
            pass
        chapter_list.append(chapter)
    buy_df = pd.DataFrame({'user_id': user_id_list, 'ds': ds_list, 'chapter': chapter_list})
    buy_df = buy_df.groupby(['ds', 'chapter']).user_id.count().reset_index().rename(columns={'user_id': 'buy_num'})
    print buy_df.head(10)

    # 结果
    result_df = data.merge(buy_df, on=['ds', 'chapter'], how='left').fillna(0)
    print result_df.head(10)
    return result_df


if __name__ == '__main__':

    for platform in ['dancer_tw', ]:
        settings_dev.set_env(platform)
        result_list = []
        for date in date_range('20161230', '20170103'):
            result = tmp_20170117_active(date)
            result_list.append(result)
        result_df = pd.concat(result_list)
        # print result_df
        result_df.to_excel(r'E:\Data\output\dancer\active_qishu_%s.xlsx' % platform)
