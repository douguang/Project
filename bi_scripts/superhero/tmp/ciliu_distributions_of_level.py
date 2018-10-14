#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
@Time    : 2017/8/15 0015 15:17
@Author  : Andy 
@File    : ciliu_distributions_of_level.py
@Software: PyCharm
Description :
'''

from utils import date_range, ds_add, hql_to_df
import settings_dev
import pandas as pd


def tw_ltv(date):
    dau_sql = '''
            select ds,count(distinct account) as dau from raw_info where ds='{today}' group by ds
    '''.format(**{'today': date, })
    dau_df = hql_to_df(dau_sql).fillna(0)
    print dau_df.head(3)

    equipment_sql = '''
        select ds,account,max(level) as level from raw_info where ds='{today}' and account not in( select account from raw_info where ds>='{tomor}'and ds<='{week}' group by account ) group by ds,account
    '''.format(**{'today':date,'tomor':ds_add(date,1),'week':ds_add(date,7)})
    equip_df = hql_to_df(equipment_sql).fillna(0)
    print equip_df.head(3)

    equip_df = pd.DataFrame(equip_df).fillna(0).groupby(
        ['ds', 'level',]).agg({
        'account': lambda g: g.nunique(),
    }).reset_index()

    reg_df = equip_df.merge(dau_df, on=['ds'], how='left').fillna(0)

    return reg_df


if __name__ == '__main__':
    res_list = []
    for platform in ['superhero_vt',]:
        settings_dev.set_env(platform)
        for date in date_range('20170801','20170814'):
            print date
            res = tw_ltv(date)
            res_list.append(res)
    pd.concat(res_list).to_excel(r'E:\superhero_vt_7day-max(account)_20170801-20170815.xlsx', index=False)
    print "end"