#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
@Time    : 17-3-8 下午3:02
@Author  : Andy 
@File    : sanguo_weekly_report_platform.py
@Software: PyCharm
Description :  机甲无双-周报-渠道数据
'''

from utils import hql_to_df, ds_add
import settings_dev
import pandas as pd
import sys
reload(sys)
sys.setdefaultencoding('utf8')

def weekly_data(platform, date):
    if platform in ('sanguo_ks', 'sanguo_tw', 'sanguo_tl'):
        # 三国
        sql = '''
        select '{platform}' as youxi,t1.user_id, t1.ds, t1.regtime, t1.platform, t2.pay from (
            select user_id, ds, regexp_replace(substr(reg_time,1,10),'-','') as regtime, platform from raw_info where ds>='{date_7}' and ds<='{date}') t1
            left join (
            select user_id, ds, sum(order_money) as pay from raw_paylog where ds>='{date_7}' and ds<='{date}' and platform_2<>'admin_test' group by user_id, ds) t2
            on (t1.ds = t2.ds and t1.user_id= t2.user_id)
        '''.format(date=date, date_7=ds_add(date, -6), platform=platform)

    if platform in ('dancer_pub', 'dancer_tw'):
        # 武娘
        sql = '''
        select '{platform}' as youxi,t1.user_id, t1.ds, t1.regtime, t3.platform, t2.pay from (
            select user_id, ds, regexp_replace(substr(reg_time,1,10),'-','') as regtime from parse_info where ds>='{date_7}' and ds<='{date}') t1
            left join (
            select user_id, ds, sum(order_money) as pay from raw_paylog where ds>='{date_7}' and ds<='{date}' and platform_2<>'admin_test' group by user_id, ds) t2
            on (t1.ds = t2.ds and t1.user_id= t2.user_id) left join (
            select user_id, ds, platform from parse_actionlog where ds>='{date_7}' and ds<='{date}' group by user_id, ds, platform) t3
            on (t1.ds = t3.ds and t1.user_id= t3.user_id)
        '''.format(date=date, date_7=ds_add(date, -6), platform=platform)
        print sql

    if platform in ('superhero_tw', 'superhero_pub'):
        # 超级英雄
        sql = '''
                select '{platform}' as youxi,t1.user_id, t1.ds, t1.regtime, t3.platform, t2.pay from (
                    select uid as user_id, ds, regexp_replace(substr(create_time,1,10),'-','') as regtime from raw_info where ds>='{date_7}' and ds<='{date}') t1
                    left join (
                    select uid as user_id, ds, sum(order_money) as pay from raw_paylog where ds>='{date_7}' and ds<='{date}' and platform_2<>'admin_test' group by user_id, ds) t2
                    on (t1.ds = t2.ds and t1.user_id= t2.user_id) left join (
                    select uid as user_id, ds, platform_2  as platform from mid_info_all where ds>='{date_7}' and ds<='{date}' group by user_id, ds, platform) t3
                    on (t1.ds = t3.ds and t1.user_id= t3.user_id)
                '''.format(date=date, date_7=ds_add(date, -6), platform=platform)
        print sql


    df = hql_to_df(sql).fillna(0)
    #print df.head(10)
    dau_df = df.groupby(['youxi', 'platform']).user_id.nunique().reset_index().rename(columns={'user_id': 'wau'})
    #print dau_df.head(10)
    dnu_df = df[df['ds'] == df['regtime']].groupby(['youxi', 'platform']).user_id.nunique().reset_index().rename(columns={'user_id': 'wnu'})
    #print dnu_df.head(10)
    pay_num_df = df[df['pay'] != 0].groupby(['youxi', 'platform']).user_id.nunique().reset_index().rename(columns={'user_id': 'pay_num'})
    #print pay_num_df.head(10)
    pay_df = df.groupby(['youxi', 'platform']).pay.sum().reset_index()
    #print pay_df.head(10)
    data_df = (dau_df.merge(dnu_df, on=['youxi', 'platform'], how='left').merge(pay_num_df, on=['youxi', 'platform'], how='left')
               .merge(pay_df, on=['youxi', 'platform'], how='left').fillna(0))
    #print data_df
    data_df = data_df[data_df['platform'] != 'test']
    return data_df

if __name__ == '__main__':
    date = '20171108'
    daily_date_14 = []
    for platform in ['sanguo_ks',]:
        settings_dev.set_env(platform)
        this_week = date
        last_week = ds_add(this_week,-7)
        platform_list = []
        if this_week:
            res = weekly_data(platform,this_week)
            res_this_week = pd.DataFrame(res)
            res_this_week['pay'] = res_this_week['pay'].astype(int)
            # res_this_week.to_excel(r'/home/kaiqigu/桌面/week_report_platform_this.xlsx', index=False)
            # 添加渠道排名
            res_this_week = res_this_week.sort_values(by=['pay'], ascending=False)
            res_this_week['rank'] = range(1, (len(res_this_week) + 1))
            print res_this_week.head()

            # 取本周排名前9的渠道
            plat_df = res_this_week.sort_values(by='pay',ascending=False).head(9).platform
            platform_list = pd.np.array(plat_df)
            print platform_list

            # 取本周所有的渠道
            plat_all_df = res_this_week.platform
            plat_all_df = plat_all_df.drop_duplicates()
            platform_all_list = pd.np.array(plat_all_df)
            print platform_all_list

            # 将非前9的渠道替换为other
            platform_dff_list = list(set(platform_all_list).difference(set(platform_list)))
            print platform_dff_list
            if platform_dff_list.__len__() != 0:
                res_this_week['platform']= res_this_week.replace(platform_dff_list, 'other').platform
            res_this_week.fillna(0)

            res_this_week = pd.DataFrame(res_this_week).groupby(['youxi', 'platform',]).agg({
                'wau': lambda g: g.sum(),
                'wnu': lambda g: g.sum(),
                'pay_num': lambda g: g.sum(),
                'pay': lambda g: g.sum(),
                'rank':lambda g: g.sum(),
            }).reset_index()
            res_this_week['ARPU'] = res_this_week['pay'] / res_this_week['wau']

        if last_week:
            res = weekly_data(platform, last_week)
            res_last_week = pd.DataFrame(res)
            res_last_week['pay'] = res_last_week['pay'].astype(int)
            # res_last_week.to_excel(r'/home/kaiqigu/桌面/week_report_platform_last.xlsx', index=False)
            # 添加渠道排名
            res_last_week = res_last_week.sort_values(by=['pay'], ascending=False)
            res_last_week['rank'] = range(1, (len(res_last_week) + 1))
            print res_last_week.head()

            # 取上周所有的渠道
            plat_all_df = res_last_week.platform
            plat_all_df = plat_all_df.drop_duplicates()
            platform_all_list = pd.np.array(plat_all_df)
            print platform_all_list

            # 将非前9的渠道替换为other
            platform_dff_list = list(set(platform_all_list).difference(set(platform_list)))
            print platform_dff_list
            if platform_dff_list.__len__() != 0:
                res_last_week['platform'] = res_last_week.replace(platform_dff_list, 'other').platform
            res_last_week.fillna(0)

            res_last_week = res_last_week.groupby(['youxi', 'platform', ]).agg({
                'wau': lambda g: g.sum(),
                'wnu': lambda g: g.sum(),
                'pay_num': lambda g: g.sum(),
                'pay': lambda g: g.sum(),
                'rank': lambda g: g.sum(),
            }).reset_index()
            res_last_week['ARPU_l'] = res_last_week['pay'] / res_last_week['wau']
            res_last_week = res_last_week.rename(columns={'wau': 'wau_l', 'wnu': 'wnu_l', 'pay_num': 'pay_num_l', 'pay': 'pay_l','rank':'rank_l', })

        fina_res = pd.DataFrame(res_this_week).merge(res_last_week,on=['youxi', 'platform'],how='left')

        daily_date_14.append(fina_res)

    result = pd.concat(daily_date_14)

    result['wau_rate'] = (result['wau']-result['wau_l'])/result['wau_l']
    result['wnu_rate'] = (result['wnu']-result['wnu_l'])/result['wnu_l']
    result['pay_rate'] = (result['pay']-result['pay_l'])/result['pay_l']
    result['pay_num_rate'] = (result['pay_num']-result['pay_num_l'])/result['pay_num_l']

    result['wau_rate'] = result.replace('Inf', 0).wau_rate
    result['wnu_rate'] = result.replace('Inf', 0).wnu_rate
    result['pay_rate'] = result.replace('Inf', 0).pay_rate
    result['pay_num_rate'] = result.replace('Inf', 0).pay_num_rate


    result = pd.DataFrame(result).fillna(0)
    result.to_excel(r'/Users/kaiqigu/Documents/Sanguo/三国周报数据/week_report_platform_1.xlsx', index=False)
    res_a = result[['youxi','platform','wau_l','wau','wau_rate',]]
    res_a = res_a.rename(columns={'wau_l': '上周活跃', 'wau': '本周活跃', 'wau_rate': '环比',})
    res_a.to_excel(r'/Users/kaiqigu/Documents/Sanguo/三国周报数据/week_report_platform_1.xlsx', index=False)

    res_b = result[['youxi', 'platform', 'wnu_l', 'wnu', 'wnu_rate',]]
    res_b = res_b.rename(columns={'wnu_l': '上周新增', 'wnu': '本周新增', 'wnu_rate': '环比', })
    res_b.to_excel(r'/Users/kaiqigu/Documents/Sanguo/三国周报数据/week_report_platform_2.xlsx', index=False)

    result['rank_change'] = result['rank_l'] - result['rank']
    result = result[['youxi','platform','rank','pay','pay_l','pay_rate','rank_l','rank_change','ARPU','ARPU_l',]]
    result = result.sort_values(by='rank',ascending=True)
    result = result.T
    result['标题',]=['游戏','渠道','本周排名','本周收入','上周收入','收入环比','上周排名','排名变化','本周ARPU','上周ARPU',]
    result.to_excel(r'/Users/kaiqigu/Documents/Sanguo/三国周报数据/week_report_platform_3.xlsx', index=False)
    print 'end'

