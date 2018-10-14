#!/usr/bin/env python  
# encoding: utf-8  

""" 
@author: Andy 
@site:  
@software: PyCharm 
@file: limit_hero.py 
@time: 17/9/8 下午5:39 
"""

import pandas as pd
from pandas import DataFrame
from utils import get_active_conf, hql_to_df, ds_short, update_mysql, date_range
import settings_dev
import  time
def dis_activity_limit_hero():

    info_sql = '''
        select ds,user_id,reverse(substr(reverse(user_id), 8)) as server,max(vip_level) as vip,a_tar,log_t
        from parse_actionlog
        WHERE ds = '20170714'
        and a_typ  =  'card_gacha.do_limit_gacha'
        and a_tar like '%gacha_id%'
        group by ds,user_id,reverse(substr(reverse(user_id), 8)),a_tar,log_t
    '''

    info_df = hql_to_df(info_sql)
    # info_df.to_excel('/home/kaiqigu/桌面/机甲无双-泰国-VIP.xlsx', index=False)

    vip_sql = '''
        select ds,user_id,max(vip) as vip from raw_info where ds='20170714'group by ds,user_id
    '''
    vip_df = hql_to_df(vip_sql)

    info_df = info_df[['ds','user_id', 'server', 'vip', 'a_tar', ]]
    result_df = info_df.fillna(0)
    result_df = pd.DataFrame(result_df).reindex()

    if result_df.__len__() != 0:

        ds_list, user_id_list, service_id_list,vip_list, gacha_id_list,a_tar_list = [], [], [], [], [],[]
        for i in range(len(result_df)):
            ds = result_df.iloc[i, 0]
            user_id = result_df.iloc[i, 1]
            service_id = result_df.iloc[i, 2]
            vip = result_df.iloc[i, 3]
            tar = result_df.iloc[i, 4]

            tar = eval(tar)
            gacha_id = tar['gacha_id']
            times = tar['times']

            ds_list.append(ds)
            user_id_list.append(user_id)
            service_id_list.append(service_id)
            vip_list.append(vip)
            gacha_id_list.append(gacha_id)
            a_tar_list.append(int(times))

        data = DataFrame({'ds': ds_list,
                          'user_id': user_id_list,
                          'server': service_id_list,
                          'vip': vip_list,
                          'gacha_id': gacha_id_list,
                          'times': a_tar_list,
                          })

        mid_a_df = data[data['times'] == 10]
        mid_a_df['core']=110
        mid_b_df = data[data['times'] == 1]
        mid_b_df['core'] = 10
        mid_result=[]
        mid_result.append(mid_a_df)
        mid_result.append(mid_b_df)
        data = pd.concat(mid_result).reindex()

        result1 = data.groupby(['ds', 'user_id','server','vip',]).agg(
            {'times': lambda g: g.sum()}).reset_index()

        result2 = data.groupby(['ds', 'user_id', 'server', 'vip', ]).agg(
            {'core': lambda g: g.sum()}).reset_index()
        result = DataFrame(result1).merge(result2,on=['ds', 'user_id', 'server', 'vip',],how='left')

        result = DataFrame(result).reindex()
        result['ds'] = result['ds'].astype("str")
        result['user_id'] = result['user_id'].astype("str")
        result['server'] = result['server'].astype("str")
        result['vip'] = result['vip'].astype("int")
        result['times'] = result['times'].astype("int")
        result['core'] = result['core'].astype("int")
        result_df = result.fillna(0)
        result_df = DataFrame(result_df)

        result_df = result_df.sort_values(by=['core'], ascending=False)
        result_df['rank'] = range(1, (len(result_df) + 1))
        result_df['rank'] = result_df['rank'].astype("int")
        # result_df = result_df[result_df['rank'] <= 500]

        money_spend_sql = '''
              select ds,user_id,sum(order_money) as money
              from raw_paylog
              WHERE ds = '20170714'
              and platform_2 !="admin_test"
              group by ds,user_id
        '''

        money_spend_df = hql_to_df(money_spend_sql)
        money_spend_df = money_spend_df.fillna(0)
        result_df = result_df.merge(money_spend_df,on=['ds','user_id'],how='left')
        result_df =DataFrame(result_df).fillna(0)

        result_df['money'] = result_df['money'].astype("int")

        coin_spendlog = '''
            select ds,user_id,sum(coin_num) as coin_spend from raw_spendlog where ds='20170714'  group by ds,user_id
        '''
        coin_spendlog_df = hql_to_df(coin_spendlog)
        result_df = result_df.merge(coin_spendlog_df, on=['ds', 'user_id'], how='left')
        result_df = DataFrame(result_df).fillna(0)

        result_df = result_df.merge(coin_spendlog_df, on=['ds', 'user_id'], how='left')
        result_df = DataFrame(result_df).fillna(0)

        result_df.to_excel('/home/kaiqigu/桌面/机甲无双-金山-限时神降_20170717-2.xlsx', index=False)
if __name__ == '__main__':
    for platform in ['sanguo_ks',]:
        settings_dev.set_env(platform)
        result = dis_activity_limit_hero()
    print "end"



