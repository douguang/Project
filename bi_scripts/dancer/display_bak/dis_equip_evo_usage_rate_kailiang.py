#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
@Time    : 16-10-27 上午11:36
@Author  : kailiang Li
@File    : dis_equip_evo_usage_rate.py
@Software: PyCharm
Description :  武娘  装备的使用率和装备进阶数据
'''
import settings_dev
from utils import ds_add, hql_to_df, update_mysql, get_config, date_range
import pandas as pd
from pandas import DataFrame


def dis_equip_reduce(date):
    equip_sql = '''
        SELECT reverse(substr(reverse(user_id),8)) AS server,
               user_id,
               vip,
               equip_dict
        FROM mid_info_all
        WHERE ds = '{date}'
        '''.format(date=date)
    print equip_sql
    card_df = hql_to_df(equip_sql)
    # 获取装备配置
    info_config = get_config('equip_info')
    detail_config = get_config('equip')

    equip_pos = set(range(1, 10, 1))

    def card_evo_lines():
        for _, row in card_df.iterrows():
            for cards_id, card_info in eval(row['equip_dict']).iteritems():
                card_id = str(card_info['c_id'])
                character_id = str(detail_config.get(
                    card_id, {}).get('equip_id'))
                card_name = info_config.get(character_id, {}).get('name')
                if card_info['pos'] in equip_pos:
                    attend_num = 1
                else:
                    attend_num = 0
                quality = detail_config.get(card_id, {}).get('quality')
                step = detail_config.get(card_id, {}).get('step')
                yield [date, row.server, row.user_id, row.vip, character_id, card_name, attend_num, quality, step, card_id]

    card_info_df = pd.DataFrame(
        card_evo_lines(),
        columns=[
            'ds',
            'server',
            'user_id',
            'vip',
            'character_id',
            'card_name',
            'attend_num',
            'quality',
            'step',
            'card_id'])
    card_info_df['num'] = 1

    # 解决进阶的因数据确实的错误：
    # 计算红色和橙色的step的最大值
    red_max_step = card_info_df[card_info_df['quality'] == 6].step.max()
    #print card_info_df['step'].max()

    # 拥有人数
    own_df = card_info_df.groupby(['ds', 'server', 'vip', 'character_id', 'card_name']).agg(
        {'user_id': lambda g: g.nunique()}).reset_index()
    own_df = own_df.rename(columns={'user_id': 'have_user_num', })
    own_df = own_df[
        ['ds', 'server', 'vip', 'character_id', 'card_name', 'have_user_num']]

    # 总件数
    sum_df = card_info_df.groupby(['ds', 'server', 'vip', 'character_id', 'card_name']).agg(
        {'num': lambda g: g.sum()}).reset_index()
    sum_df = sum_df.rename(columns={'num': 'all_num', })
    sum_df = sum_df[['ds', 'server', 'vip',
                     'character_id', 'card_name', 'all_num']]

    # 上阵件数　　
    attend_df = card_info_df.groupby(['ds', 'server', 'vip', 'character_id', 'card_name']).agg(
        {'attend_num': lambda g: g.sum()}).reset_index()
    attend_df = attend_df.rename(columns={'attend_num': 'attend_sum', })
    attend_df = attend_df[
        ['ds', 'server', 'vip', 'character_id', 'card_name', 'attend_sum']]

    # 上阵率  = 上阵件数/总件数
    attend_rate_mid_df = sum_df.merge(
        attend_df, on=[
            'ds', 'server', 'vip', 'character_id', 'card_name'], how='left')
    attend_rate_mid_df = DataFrame(attend_rate_mid_df).fillna(0)
    attend_rate_mid_df["attend_rate"] = attend_rate_mid_df[
        "attend_sum"] * 1.0 / attend_rate_mid_df["all_num"]
    attend_rate_df = attend_rate_mid_df[
        ['ds', 'server', 'vip', 'character_id', 'card_name', 'attend_rate']]

    # 橙色上阵人数
    org_attend_is = card_info_df[card_info_df['quality'] == 5].count().quality
    if not org_attend_is == 0:
        org_attend_mid_df = card_info_df[card_info_df['quality'] == 5]
        # 　用统计做判空操作
        attend_num_is = org_attend_mid_df[
            org_attend_mid_df['attend_num'] == 1].count().attend_num
        if not attend_num_is == 0:
            org_attend_mid_df = org_attend_mid_df[
                org_attend_mid_df['attend_num'] == 1]
            org_attend_df = org_attend_mid_df.groupby(['ds', 'server', 'vip', 'character_id', 'card_name']).agg(
                {'user_id': lambda g: g.nunique()}).reset_index()
            org_attend_df = org_attend_df.rename(
                columns={'user_id': 'org_attend_player_num', })
        else:
            org_attend_mid_df['org_attend_player_num'] = 0
            org_attend_df = org_attend_mid_df
    else:
        org_attend_mid_df = card_info_df[card_info_df['quality'] == 5]
        org_attend_mid_df['org_attend_player_num'] = 0
        org_attend_df = org_attend_mid_df
    org_attend_df = org_attend_df[
        ['ds', 'server', 'vip', 'character_id', 'card_name', 'org_attend_player_num']]
    #　汇总
    org_attend_df = org_attend_df.groupby(['ds', 'server', 'vip', 'character_id', 'card_name']).agg(
        {'org_attend_player_num': lambda g: g.sum()}).reset_index()

    # 红色上阵人数
    red_attend_is = card_info_df[card_info_df['quality'] == 6].count().quality
    if not red_attend_is == 0:
        red_attend_mid_df = card_info_df[card_info_df['quality'] == 6]
        # print "ddddd",red_attend_mid_df
        #red_attend_mid_df.to_excel("/home/kaiqigu/红色所有信息.xlsx", index=False)
        #　用统计做判空操作
        attend_num_is = red_attend_mid_df[
            red_attend_mid_df['attend_num'] == 1].count().attend_num
        if not attend_num_is == 0:
            red_attend_mid_df = red_attend_mid_df[
                red_attend_mid_df['attend_num'] == 1]
            red_attend_df = red_attend_mid_df.groupby(['ds', 'server', 'vip', 'character_id', 'card_name']).agg(
                {'user_id': lambda g: g.nunique()}).reset_index()
            # print "dddddd", red_attend_df
            red_attend_df = red_attend_df.rename(
                columns={'user_id': 'red_attend_player_num', })
            # print "dsdddd", red_attend_df

        else:
            red_attend_mid_df['red_attend_player_num'] = 0
            red_attend_df = red_attend_mid_df
    else:
        red_attend_mid_df = card_info_df[card_info_df['quality'] == 6]
        red_attend_mid_df['red_attend_player_num'] = 0
        red_attend_df = red_attend_mid_df

    red_attend_df = red_attend_df[
        ['ds', 'server', 'vip', 'character_id', 'card_name', 'red_attend_player_num']]
    red_attend_df = red_attend_df.groupby(['ds', 'server', 'vip', 'character_id', 'card_name']).agg(
        {'red_attend_player_num': lambda g: g.sum()}).reset_index()
    print "红色上阵人数", red_attend_df

    # 橙色总件数
    org_mid_df = card_info_df[card_info_df['quality'] == 5]
    org_df = org_mid_df.groupby(['ds', 'server', 'vip', 'character_id', 'card_name']).agg(
        {'num': lambda g: g.sum()}).reset_index()
    org_df = org_df.rename(columns={'num': 'org_all_num', })
    org_df = org_df[
        ['ds', 'server', 'vip', 'character_id', 'card_name', 'org_all_num']]
    # 红色总件数
    red_df = card_info_df[card_info_df['quality'] == 6]
    red_df = red_df.groupby(['ds', 'server', 'vip', 'character_id', 'card_name']).agg(
        {'num': lambda g: g.sum()}).reset_index()
    red_df = red_df.rename(columns={'num': 'red_all_num', })
    #red_df = pd.DataFrame(red_df)
    red_df = red_df[
        ['ds', 'server', 'vip', 'character_id', 'card_name', 'red_all_num']]

    # 红+0 红+1 红+3 红+4 红+10 红+20
    red_mid_df = card_info_df[card_info_df['quality'] == 6]
    red_add_df = pd.pivot_table(
        red_mid_df, index=[
            'ds', 'server', 'vip', 'character_id', 'card_name'], columns=[
            'quality', 'step'], aggfunc={
                'num': sum}, fill_value=0).reset_index()
    red_add_df.columns = ['ds', 'server', 'vip', 'character_id',
                          'card_name'] + ['red_%r' % r for r in range(red_max_step+1)]
    red_add_df = red_add_df.groupby(
        ['ds', 'server', 'vip', 'character_id', 'card_name']).sum().reset_index()

    # 添加拥有人数
    # 总件数
    result = sum_df.merge(
        own_df, on=[
            'ds', 'server', 'vip', 'character_id', 'card_name'], how='left')
    result = DataFrame(result).fillna(0)
    # 上阵件数　　
    result = result.merge(
        attend_df, on=[
            'ds', 'server', 'vip', 'character_id', 'card_name'], how='left')
    result = DataFrame(result).fillna(0)
    # 上阵率
    result = result.merge(
        attend_rate_df, on=[
            'ds', 'server', 'vip', 'character_id', 'card_name'], how='left')
    result = DataFrame(result).fillna(0)
    # 橙色上阵人数
    result = result.merge(
        org_attend_df, on=[
            'ds', 'server', 'vip', 'character_id', 'card_name'], how='left')
    result = DataFrame(result).fillna(0)
    # 红色上阵人数
    result = result.merge(
        red_attend_df, on=[
            'ds', 'server', 'vip', 'character_id', 'card_name'], how='left')
    result = DataFrame(result).fillna(0)
    # 橙色总件数
    result = result.merge(
        org_df, on=[
            'ds', 'server', 'vip', 'character_id', 'card_name'], how='left')
    result = DataFrame(result).fillna(0)
    # 红色总件数
    result = result.merge(
        red_df, on=[
            'ds', 'server', 'vip', 'character_id', 'card_name'], how='left')
    result = DataFrame(result).fillna(0)
    # 红+0 红+1 红+3 红+4 红+10 红+41
    result = result.merge(
        red_add_df, on=[
            'ds', 'server', 'vip', 'character_id', 'card_name'], how='left')
    result = DataFrame(result)
    result = DataFrame(result).fillna(0)
    #　重命名
    result = result.rename(
        columns={
            'character_id': 'equip_id',
            'card_name': 'equip_name',
        })
    result.fillna(0)

    # 修改数据类型
    column_list = ['vip',
                   'equip_id',
                   'all_num',
                   'have_user_num',
                   'attend_sum',
                   'attend_rate',
                   'org_attend_player_num',
                   'red_attend_player_num',
                   'org_all_num',
                   'red_all_num'] + ['red_%r' % r for r in range(red_max_step + 1)]
    for i in (red_max_step + 1, 20):
        key_in = 'red_%r' % i
        result[key_in] = 0
        result.reindex()

    result[column_list] = result[column_list].astype(int)
    print result
    table = 'dis_equip'
    # 更新MySQL
    del_sql = 'delete from {0} where ds="{1}"'.format(table, date)
    update_mysql(table, result, del_sql)

if __name__ == '__main__':
    for platform in ['dancer_pub','dancer_tw']:
        settings_dev.set_env(platform)
        for date in date_range('20161110', '20161112'):
            dis_equip_reduce(date)
        #print "dddd"
    print "game over"

