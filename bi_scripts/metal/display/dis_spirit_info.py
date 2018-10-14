#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Hu Wenchao
Description : 魂灵等级分布
'''
import pandas as pd
from collections import Counter
from utils import hql_to_df, update_mysql, ds_add, get_config
import settings_dev

spirit_lv_range = range(1, 31)

def dis_spirit_info(date):
    spirit_info_sql = '''
SELECT user_id,
       vip,
       spirit_dict
FROM mid_info_all
WHERE ds = '{date}'
  AND regexp_replace(substr(act_time,1,10),'-','') >= '{date_in_3days}'
    '''.format(**{
        'date': date,
        'date_in_3days': ds_add(date, -2),
    })
    # print spirit_info_sql
    spirit_info_df = hql_to_df(spirit_info_sql)
    print spirit_info_df

    def spirit_lines():
        for _, row in spirit_info_df.iterrows():
            for spirit_id, spirit_info in eval(row.spirit_dict).iteritems():
                yield [row.user_id, row.vip, spirit_id, spirit_info['lv']]

    spirit_all_df = pd.DataFrame(spirit_lines(),
                                 columns=['user_id', 'vip', 'spirit_id', 'lv'])
    spirit_all_df['server'] = spirit_all_df['user_id'].map(lambda s: s[:-7])
    # 分组聚合
    spirit_grouped_df = spirit_all_df.groupby(['server', 'vip', 'spirit_id']).agg({
        'lv': lambda g: tuple(g)
    }).reset_index()

    spirit_cfg = get_config('spirit_skill_detail')
    spirit_grouped_df['spirit_name'] = spirit_all_df['spirit_id'].map(
        lambda s: spirit_cfg[str(s)]['name'])
    spirit_grouped_df['lv_counter'] = spirit_grouped_df['lv'].map(
        lambda x: Counter(x))
    spirit_grouped_df['spirit_num'] = spirit_grouped_df['lv'].map(lambda x: len(x))
    for lv in spirit_lv_range:
        spirit_grouped_df['lv_{0}'.format(lv)] = spirit_grouped_df[
            'lv_counter'].map(lambda s: s.get(lv, 0))
    print spirit_grouped_df
    spirit_grouped_df['ds'] = date
    columns = ['ds', 'server', 'vip', 'spirit_id', 'spirit_name', 'spirit_num'
               ] + ['lv_%d' % i for i in spirit_lv_range]

    # 更新MySQL表
    table = 'dis_spirit_info'
    del_sql = 'delete from {0} where ds="{1}"'.format(table, date)
    update_mysql(table, spirit_grouped_df[columns], del_sql)

    # 近三天进入真国战的uid
    uids_entered_cw_sql = '''
SELECT ds,
       user_id
FROM parse_actionlog
WHERE ds >= '{date_in_3days}'
  AND ds <= '{date}'
  AND a_typ = 'country_war_1.whole_map'
  AND return_code = ''
    '''.format(**{
        'date': date,
        'date_in_3days': ds_add(date, -2),
    })
    uids_entered_cw_df = hql_to_df(uids_entered_cw_sql).drop_duplicates()

    uids_cw_vip_df = uids_entered_cw_df.merge(spirit_info_df[['user_id', 'vip']], on='user_id')
    uids_cw_vip_df['server'] = uids_cw_vip_df.user_id.map(lambda s: s[:-7])
    # 三日活跃并进入真国战人数
    cw_3d_act_df = (uids_cw_vip_df
                    .groupby(['server', 'vip'])
                    .user_id
                    .nunique()
                    .reset_index()
                    .rename(columns={'user_id': 'cw_3d_act'})
                    )
    # 当日活跃并进入真国战人数
    cw_today_act_df = (uids_cw_vip_df
                       .loc[uids_cw_vip_df.ds == date]
                       .groupby(['server', 'vip'])
                       .user_id
                       .nunique()
                       .reset_index()
                       .rename(columns={'user_id': 'cw_today_act'})
                       )

    # 当日活跃魂灵购买情况
    buy_spirit_sql = '''
SELECT user_id,
       a_tar,
       freemoney_diff,
       money_diff
FROM parse_actionlog
WHERE ds = '{date}'
  AND a_typ = 'spirit_skill.buy_spirit'
  AND return_code = ''
    '''.format(date=date)
    buy_spirit_df = hql_to_df(buy_spirit_sql)
    buy_spirit_df = buy_spirit_df.fillna(0)
    buy_spirit_df['buy_spirit_times'] = buy_spirit_df.a_tar.map(lambda s: int(eval(s).get('buy_amount', 0)))
    buy_spirit_df['consume_money'] = -(buy_spirit_df.freemoney_diff + buy_spirit_df.money_diff)
    buy_spirit_df['server'] = buy_spirit_df.user_id.map(lambda s: s[:-7])
    buy_spirit_vip_df = buy_spirit_df.merge(spirit_info_df[['user_id', 'vip']], on='user_id')
    buy_spirit_result_df = buy_spirit_vip_df.groupby(['server', 'vip']).agg({
        'consume_money': lambda s: s.sum(),
        'buy_spirit_times': lambda s: s.count(),
        'user_id': lambda s: s.nunique()
    }).reset_index().rename(columns={'user_id': 'buy_user_num'})

    # 整合活跃且进入真国战人数
    spirit_buy_info_df = (buy_spirit_result_df
                          .merge(cw_3d_act_df, on=['server', 'vip'], how='outer')
                          .merge(cw_today_act_df, on=['server', 'vip'], how='outer')
                          .fillna(0)
                          )
    spirit_buy_info_df['ds'] = date
    # 更新MySQL
    table = 'dis_spirit_buy_info'
    del_sql = 'delete from {0} where ds="{1}"'.format(table, date)
    update_mysql(table, spirit_buy_info_df, del_sql)

if __name__ == '__main__':
    settings_dev.set_env('metal_test')
    dis_spirit_info('20160521')
