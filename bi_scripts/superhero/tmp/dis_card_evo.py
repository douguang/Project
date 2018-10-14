#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Dong Junshuang
Description : 卡牌进阶
'''
from utils import hqls_to_dfs, update_mysql, get_config
import settings
import pandas as pd

card_list = [55, 1100, 1900, 2200, 3100, 3500, 4100, 4200, 4400, 4800, 6200,
             6300, 6400, 6500, 6700, 6800, 6900, 7000, 7100, 7300, 7600, 7700,
             7800, 7900, 8100, 8200, 8300, 8400, 8500, 8600, 8700, 8800, 8900,
             9000, 9100, 9200, 9300, 9400, 9500, 9600, 9700, 9800, 9900, 32000,
             32100, 32200, 32700, 32800, 32900]
if __name__ == '__main__':
    settings.set_env('superhero_vt')
    date = '20160801'
    plat = None
    # def dis_card_evo_one(date,plat=None):
    plat = plat or settings.platform

    jinjie_num = [i for i in range(0, 61)]
    jinjie_dic = {num: 'd%d_num' % num for num in jinjie_num}
    card_sql = '''
    SELECT uid,
           reverse(substring(reverse(uid), 8)) AS server,
           card_id,
           is_fight,
           jinjie
    FROM raw_card
    WHERE ds = '{0}'
    '''.format(date)
    info_sql = '''
    SELECT uid,
           vip_level
    FROM raw_info
    WHERE ds='{0}'
    '''.format(date)
    if plat == 'superhero_pub':
        print 'superhero_pub'
        st_sql = "and substr(uid,1,1) = 'g'"
        card_sql = card_sql + st_sql
    if plat == 'superhero_ios':
        print 'superhero_ios'
        st_sql = "and substr(uid,1,1) = 'a'"
        card_sql = card_sql + st_sql
    card_df, info_df = hqls_to_dfs([card_sql, info_sql])
    card_result_df = card_df.merge(info_df, on='uid', how='left')
    # 拥有人数
    have_num_df = (card_result_df.groupby(['uid', 'card_id'])
                   .count().reset_index().groupby(['card_id'])
                   .count().reset_index().loc[:, ['card_id', 'uid']]
                   .rename(columns={'uid': 'have_num'}))
    # 上阵数量
    fight_num_data = card_result_df[card_result_df['is_fight'] == 1]
    fight_num_df = (fight_num_data.groupby(['uid', 'card_id'])
                    .count().reset_index().groupby(['card_id'])
                    .count().reset_index().loc[:, ['card_id', 'uid']]
                    .rename(columns={'uid': 'fight_num'}))
    # 进阶
    card_result_df['jinjie_num'] = 1
    jinjie_df = (
        card_result_df.groupby(['card_id', 'jinjie']).count().reset_index()
        .loc[:, ['card_id', 'jinjie', 'jinjie_num']].pivot_table(
            'jinjie_num', ['card_id'],
            'jinjie').reset_index().fillna(0).rename(columns=jinjie_dic))

    for i in ['d%d_num' % num for num in jinjie_num]:
        if i not in jinjie_df.columns:
            jinjie_df[i] = 0

    # 卡牌使用率
    result_df = (have_num_df.merge(
        fight_num_df, on=['card_id'], how='left')
                 .merge(jinjie_df, on=['card_id'],
                        how='left').fillna(0))
    result_df['have_rate'] = result_df['fight_num'] * 1.0 / result_df[
        'have_num']

    # 卡牌名
    card_id_list = []
    card_name_list = []
    result_df['card_id_str'] = result_df['card_id'].map(lambda s: str(s))
    character_detail_config = get_config('character_detail')
    for i, row in result_df.iterrows():
        card_name = character_detail_config.get('%s' % row.card_id_str,
                                                {}).get('name')
        card_id_list.append(row.card_id)
        card_name_list.append(card_name)
    card_name_df = pd.DataFrame({'card_id': card_id_list,
                                 'card_name': card_name_list})
    card_name_df = card_name_df.drop_duplicates(['card_id', 'card_name'])

    result_df = result_df.merge(card_name_df, on='card_id', how='left')
    result_df = result_df.fillna(0)
    result_df['ds'] = date
    result_df['is_card_id'] = result_df['card_id'].isin(card_list)
    result_df = result_df[result_df['is_card_id']]

    columns = ['ds', 'card_id', u'card_name', 'have_num', 'fight_num',
               'have_rate'] + ['d%d_num' % num for num in jinjie_num]
    result_df = result_df[columns]
    rename_dic = {'name': 'card_name',
                  'have_num': 'have_user_num',
                  'fight_num': 'attend_num',
                  'have_rate': 'use_rate'}
    rename_dic2 = {'d%d_num' % num: 'd%d_evo_num' % num for num in jinjie_num}
    result_df = result_df.rename(columns=rename_dic).rename(
        columns=rename_dic2)
    print result_df
    result_df.to_excel('/Users/kaiqigu/Downloads/Excel/dis_card_evo.xlsx')

# def dis_card_evo(date):
#     if settings.code_dir == 'superhero_bi':
#         for plat in ['superhero_pub', 'superhero_ios']:
#             print plat
#             dis_card_evo_one(date, plat)
#     else:
#         dis_card_evo_one(date)

# if __name__ == '__main__':
#     settings.set_env('superhero_vt')
#     date = '20160617'
#     dis_card_evo_one(date)
