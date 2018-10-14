#!/usr/bin/env python  
# encoding: utf-8  

""" 
@author: Andy 
@site:  分国家留存率
@software: PyCharm 
@file: dis_country_retention.py
@time: 18/3/3 下午3:27 
"""
import pandas as pd
import settings_dev
from utils import hql_to_df
from utils import hqls_to_dfs
from utils import ds_add
from utils import date_range
from ipip import IP
import os
from utils import update_mysql

def data_reduce(date):
    start_date = ds_add(date,-29)
    end_date=date
    final_date = date
    keep_days = [2, 3,4,5,6,7,14,30,60,90]

    result_sql = '''
            select uid,ip as country from mid_info_all where ds='{final_date}' group by uid,country
        '''.format(final_date=final_date, )
    print result_sql
    dau_df = hql_to_df(result_sql)
    dau_df = dau_df.drop_duplicates('uid')

    dau_df['country'] = dau_df['country'].astype(basestring)
    IP.load(os.path.abspath("./tinyipdata_utf8.dat"))

    def ip_lines():
        for _, row in dau_df.iterrows():
            ip = row.country
            try:
                country = IP.find(ip).strip().encode("utf8")
                if '中国台湾' in country:
                    country = '台湾'
                elif '中国香港' in country:
                    country = '香港'
                elif '中国澳门' in country:
                    country = '澳门'
                elif '中国' in country:
                    country = '中国'
                yield [row.uid,country]
            except:
                country = ''
                yield [row.uid,country]

    result = pd.DataFrame(ip_lines(), columns=['uid', 'country'])
    print result.head()

    reg_sql = '''
    SELECT  ds,
            uid
       FROM raw_reg
       WHERE ds >= '{0}'
    and ds <= '{1}'
    '''.format(start_date,end_date)
    act_sql = '''
    SELECT ds,
           uid,
           platform_2
    FROM raw_info
    WHERE ds >= '{0}'
    '''.format(start_date)
    reg_df,act_df = hqls_to_dfs([reg_sql,act_sql])
    reg_df = reg_df.merge(act_df,on=['ds','uid'],how='left')

    def plat_lines(df):
        for _, row in df.iterrows():
            ds = row.ds
            uid = row.uid
            platform_2 = 'platform'
            # platform_2 = row.platform_2
            # if platform_2 == 'iosfacebook':
            #     platform_2 = 'ios'
            # elif platform_2 == 'ioskvgames':
            #     platform_2 = 'ios'
            # else:
            #     platform_2 = 'android'
            yield [ds, uid ,platform_2]
    reg_df = pd.DataFrame(plat_lines(reg_df), columns=['ds','uid' ,'platform_2'])
    act_df = pd.DataFrame(plat_lines(act_df), columns=['ds','uid' ,'platform_2'])

    reg_df = reg_df.merge(result,on='uid',how='left')
    act_df = act_df.merge(result,on='uid',how='left')

    date_list = date_range(start_date,end_date)
    reg_dates = [date for date in date_list]

    dfs = []
    for date in reg_dates:
        # print date
        reg_data = reg_df.loc[reg_df.ds == date]
        reg_num = (
            reg_data.groupby(['platform_2','country']).count().reset_index()
                    .loc[:,['platform_2','country','uid']]
                    .rename(columns={'platform_2':'platform','uid':'reg_user_num'}))
        result_df = reg_num
        keep_day_list = [ds_add(date,i-1) for i in keep_days]
        keep_day_dic = {ds_add(date,i-1):'d%d_keeprate' %i for i in keep_days}
        act_df['is_reg_ds'] = act_df['ds'].isin(keep_day_list)
        act_data = act_df[act_df['is_reg_ds']]
        act_data['act'] = 1
        for i in keep_days:
            keep_day = ds_add(date,i-1)
            # print i,keep_day
            act_data = act_df.loc[act_df.ds == keep_day]
            act_data['is_reg'] = act_data['uid'].isin(reg_data.uid.values)
            act_data = act_data[act_data['is_reg']]
            ltv_end_date = ds_add(date, i - 1)
            if ltv_end_date > final_date:
                keep_df = (
                act_data.groupby(['platform_2','country']).count().reset_index()
                    .loc[:,['platform_2','country','uid']]
                    .rename(columns={'platform_2':'platform','uid':'d%d_keeprate' %i}))
                keep_df['d%d_keeprate' %i] = 0
            else:
                keep_df = (
                act_data.groupby(['platform_2','country']).count().reset_index()
                        .loc[:,['platform_2','country','uid']]
                        .rename(columns={'platform_2':'platform','uid':'d%d_keeprate' %i}))
            result_df = result_df.merge(keep_df,on=['platform','country'],how='left')
        result_df['ds'] = date

        dfs.append(result_df)
    df = pd.concat(dfs)
    result_df = df.fillna(0)
    for i in ['d%d_keeprate' %i for i in keep_days]:
        result_df[i] = df[i]/df['reg_user_num']
    # print result_df
    columns = ['ds', 'platform', 'country', 'reg_user_num'] + ['d%d_keeprate' % d
                                             for d in keep_days]
    result_df = result_df[columns]
    result_df = result_df.fillna(0)
    print result_df.head()

    for date_demo in set(result_df.ds.values.tolist()):
        result = result_df[result_df.ds == date_demo]
        table = 'dis_country_retention'
        del_sql = 'delete from {0} where ds="{1}"'.format(table, date_demo)
        update_mysql(table, result, del_sql)

    print '{0} is complete'.format(table)

if __name__ == '__main__':
    platform = 'superhero_vt'
    settings_dev.set_env(platform)
    for date in date_range('20180310', '20180314'):
        print date
        res = data_reduce(date)

