#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
@Time    : 17-3-8 下午3:05
@Author  : Andy
@File    : sanguo_weekly_report_vip_user.py
@Software: PyCharm
Description :   机甲无双-周报-VIP数据  张永辰
'''

from utils import hql_to_df, ds_add, date_range
import settings_dev
import pandas as pd

def weekly_data(platform, date):
    # 三国 武娘
    if platform in ('dancer_pub', 'dancer_tw', 'sanguo_ks','sanguo_bt',):
        sql = '''
        select '{platform}' as youxi,t1.user_id, t1.ds, t1.regtime, t3.vip, t2.pay from (
            select user_id, ds, regexp_replace(substr(reg_time,1,10),'-','') as regtime from raw_info where ds>='{date_7}' and ds<='{date}') t1
            left join (
            select user_id, ds, sum(order_money) as pay from raw_paylog where ds>='{date_7}' and ds<='{date}' and platform_2<>'admin_test' group by user_id, ds) t2
            on (t1.ds = t2.ds and t1.user_id= t2.user_id) left join (
            select user_id, vip from mid_info_all where ds='{date}' and regexp_replace(substr(act_time,1,10),'-','')>='{date_7}'
            ) t3 on t1.user_id = t3.user_id
        '''.format(date=date, date_7=ds_add(date, -6), platform=platform)
        print sql

    # 三国 台湾和泰国
    if platform in ('sanguo_tw', 'sanguo_tl',):
        sql = '''
        select '{platform}' as youxi,t1.user_id, t1.ds, t1.regtime, t3.vip, t2.pay from (
            select user_id, ds, regexp_replace(substr(reg_time,1,10),'-','') as regtime from raw_info where ds>='{date_7}' and ds<='{date}') t1
            left join (
            select user_id, ds, sum(order_money)/5 as pay from raw_paylog where ds>='{date_7}' and ds<='{date}' and platform_2<>'admin_test' group by user_id, ds) t2
            on (t1.ds = t2.ds and t1.user_id= t2.user_id) left join (
            select user_id, vip from mid_info_all where ds='{date}' and regexp_replace(substr(act_time,1,10),'-','')>='{date_7}'
            ) t3 on t1.user_id = t3.user_id
        '''.format(date=date, date_7=ds_add(date, -6), platform=platform)
        print sql

    if platform in ('dancer_pub', 'dancer_tw'):
        sql = sql.replace('raw_info', 'parse_info')
        print sql

    # 超级英雄
    if platform in ('superhero_tw', 'superhero_pub'):
        sql = '''
                select '{platform}' as youxi,t1.user_id, t1.ds, t1.regtime, t3.vip, t2.pay from (
                    select uid as user_id, ds, regexp_replace(substr(create_time,1,10),'-','') as regtime from raw_info where ds>='{date_7}' and ds<='{date}') t1
                    left join (
                    select uid as user_id, ds, sum(order_money) as pay from raw_paylog where ds>='{date_7}' and ds<='{date}' and platform_2<>'admin_test' group by user_id, ds) t2
                    on (t1.ds = t2.ds and t1.user_id= t2.user_id) left join (
                    select uid as user_id, vip_level as vip from mid_info_all where ds='{date}' and regexp_replace(substr(fresh_time,1,10),'-','')>='{date_7}'
                    ) t3 on t1.user_id = t3.user_id
        '''.format(date=date, date_7=ds_add(date, -6), platform=platform)
        print sql


    df = hql_to_df(sql).fillna(0)
    df = df[df['vip'] != 0]
    # print df.head(10)
    dau_df = df.groupby(['youxi', 'vip']).user_id.nunique().reset_index().rename(columns={'user_id': 'wau'})
    # print dau_df.head(10)
    dnu_df = df[df['ds'] == df['regtime']].groupby(['youxi', 'vip']).user_id.nunique().reset_index().rename(columns={'user_id': 'wnu'})
    # print dnu_df.head(10)
    pay_num_df = df[df['pay'] != 0].groupby(['youxi', 'vip']).user_id.nunique().reset_index().rename(columns={'user_id': 'pay_num'})
    # print pay_num_df.head(10)
    pay_df = df.groupby(['youxi', 'vip']).pay.sum().reset_index()
    # print pay_df.head(10)
    data_df = (dau_df.merge(dnu_df, on=['youxi', 'vip'], how='left').merge(pay_num_df, on=['youxi', 'vip'], how='left')
               .merge(pay_df, on=['youxi', 'vip'], how='left').fillna(0))
    # print data_df.head()

    return data_df

if __name__ == '__main__':
    vip_5_more = []
    vip_group = []
    daily_date_14 = []
    for platform in ['sanguo_tl','sanguo_bt','sanguo_ks',]:
    # for platform in ['dancer_pub', 'dancer_tw']:
        settings_dev.set_env(platform)
        this_week = '20171108'
        last_week = ds_add(this_week, -7)
        if this_week:
            this_week_vip = weekly_data(platform, this_week)
            this_week_vip = pd.DataFrame(this_week_vip).fillna(0)
            this_week_vip['vip'] = this_week_vip['vip'].astype(int)
            #this_week_vip.to_excel('/home/kaiqigu/桌面/week_report_vip_group_泰国本周.xlsx', index=False)
            # 本周VIP5以上的
            this_week_vip_5_more = this_week_vip[this_week_vip['vip'] >= 5]
            # 本周VIP等级分组
            vip_dic={1:'01-04',2:'01-04',3:'01-04',4:'01-04',5:'05-09',6:'05-09',7:'05-09',8:'05-09',9:'05-09',10:'10-12',11:'10-12',12:'10-12',13:'13-14',14:'13-14',15:'15'}
            this_week_vip_mid = this_week_vip
            this_week_vip_mid['vip'] = this_week_vip_mid.replace(vip_dic).vip
            this_week_vip_group = this_week_vip_mid.groupby(['youxi', 'vip', ]).agg({
                'wau': lambda g: g.sum(),
                'wnu': lambda g: g.sum(),
                'pay_num': lambda g: g.sum(),
                'pay': lambda g: g.sum(),
            }).reset_index()
            this_week_vip_group['ARPPU'] = this_week_vip_group['pay']/this_week_vip_group['pay_num']

        if last_week:
            last_week_vip = weekly_data(platform, last_week)
            last_week_vip = pd.DataFrame(last_week_vip).fillna(0)
            last_week_vip['vip'] = last_week_vip['vip'].astype(int)
            # last_week_vip.to_excel('/home/kaiqigu/桌面/week_report_vip_group_泰国上周.xlsx', index=False)
            # 本周VIP5以上的
            last_week_vip_5_more = last_week_vip[last_week_vip['vip'] >= 5]
            last_week_vip_5_more = last_week_vip_5_more.rename(columns={'wau': 'wau_l', 'wnu': 'wnu_l', 'pay_num': 'pay_num_l', 'pay': 'pay_l', })
            # 本周VIP等级分组
            vip_dic = {1: '01-04', 2: '01-04', 3: '01-04', 4: '01-04', 5: '05-09', 6: '05-09', 7: '05-09', 8: '05-09', 9: '05-09',10: '10-12', 11: '10-12', 12: '10-12', 13: '13-14', 14: '13-14',15:'15'}
            last_week_vip_mid = last_week_vip
            last_week_vip_mid['vip'] = last_week_vip_mid.replace(vip_dic).vip
            last_week_vip_group = last_week_vip_mid.groupby(['youxi', 'vip', ]).agg({
                'wau': lambda g: g.sum(),
                'wnu': lambda g: g.sum(),
                'pay_num': lambda g: g.sum(),
                'pay': lambda g: g.sum(),
            }).reset_index()
            last_week_vip_group['ARPPU_l'] = last_week_vip_group['pay'] / last_week_vip_group['pay_num']
            last_week_vip_group = last_week_vip_group.rename(columns={'wau': 'wau_l', 'wnu': 'wnu_l', 'pay_num': 'pay_num_l', 'pay': 'pay_l', })

        fina_vip_5_more = pd.DataFrame(this_week_vip_5_more).merge(last_week_vip_5_more, on=['youxi', 'vip'], how='left')
        print '----'
        print this_week_vip_group.head(3)
        print last_week_vip_group.head(3)
        fina_vip_group = pd.DataFrame(this_week_vip_group).merge(last_week_vip_group, on=['youxi', 'vip'], how='left')
        print fina_vip_5_more.head()
        print fina_vip_group.head()
        vip_5_more.append(fina_vip_5_more)
        vip_group.append(fina_vip_group)

    # VIP5以上分组
    vip_5_more_result = pd.concat(vip_5_more)
    vip_5_more_result['wau_rate'] = (vip_5_more_result['wau'] - vip_5_more_result['wau_l']) / vip_5_more_result['wau_l']
    vip_5_more_result['wnu_rate'] = (vip_5_more_result['wnu'] - vip_5_more_result['wnu_l']) / vip_5_more_result['wnu_l']
    vip_5_more_result['pay_rate'] = (vip_5_more_result['pay'] - vip_5_more_result['pay_l']) / vip_5_more_result['pay_l']
    vip_5_more_result['pay_num_rate'] = (vip_5_more_result['pay_num'] - vip_5_more_result['pay_num_l']) / vip_5_more_result['pay_num_l']

    vip_5_more_result['wau_rate'] = vip_5_more_result.replace('Inf',0).wau_rate
    vip_5_more_result['wnu_rate'] = vip_5_more_result.replace('Inf',0).wnu_rate
    vip_5_more_result['pay_rate'] = vip_5_more_result.replace('Inf',0).pay_rate
    vip_5_more_result['pay_num_rate'] = vip_5_more_result.replace('Inf',0).pay_num_rate
    vip_5_more_result = pd.DataFrame(vip_5_more_result).fillna(0)
    vip_5_more_result = vip_5_more_result[['youxi', 'vip', 'pay_l','pay','pay_rate',]]
    vip_5_more_result.to_excel('/Users/kaiqigu/Documents/Sanguo/三国周报数据/week_report_vip_group_2.xlsx', index=False)

    # VIP分组
    vip_group_result = pd.concat(vip_group)
    vip_group_result['wau_rate'] = (vip_group_result['wau'] - vip_group_result['wau_l']) / vip_group_result['wau_l']
    vip_group_result['wnu_rate'] = (vip_group_result['wnu'] - vip_group_result['wnu_l']) / vip_group_result['wnu_l']
    vip_group_result['pay_rate'] = (vip_group_result['pay'] - vip_group_result['pay_l']) / vip_group_result['pay_l']
    vip_group_result['pay_num_rate'] = (vip_group_result['pay_num'] - vip_group_result['pay_num_l']) / vip_group_result['pay_num_l']
    vip_group_result['ARPPU_rate'] = (vip_group_result['ARPPU'] - vip_group_result['ARPPU_l']) / vip_group_result['ARPPU_l']

    vip_group_result['wau_rate'] = vip_group_result.replace('Inf', 0).wau_rate
    vip_group_result['wnu_rate'] = vip_group_result.replace('Inf', 0).wnu_rate
    vip_group_result['pay_rate'] = vip_group_result.replace('Inf', 0).pay_rate
    vip_group_result['pay_num_rate'] = vip_group_result.replace('Inf', 0).pay_num_rate
    vip_group_result['ARPPU_rate'] = vip_group_result.replace('Inf', 0).ARPPU_rate
    vip_group_result = pd.DataFrame(vip_group_result).fillna(0)
    vip_group_result = vip_group_result[['youxi', 'vip', 'wau_l','wau','wau_rate','pay_num_l','pay_num','pay_num_rate','wnu_l','wnu','wnu_rate','ARPPU_l','ARPPU','ARPPU_rate',]]
    vip_group_result.to_excel('/Users/kaiqigu/Documents/Sanguo/三国周报数据/week_report_vip_group_1.xlsx', index=False)


