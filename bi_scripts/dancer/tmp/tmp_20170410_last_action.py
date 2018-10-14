#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Zhang Yongchen
Description : 武娘国服 7、8 号 cnwnioswnhd包次日流失玩家最后动作统计
'''
from utils import hql_to_df, ds_add, update_mysql, format_dates, date_range
import pandas as pd
import settings_dev
import types

def tmp_20170410_last_action(date):

    last_sql = '''
        select * from (
        select account, user_id, ds, log_t, level, vip, a_typ, row_number() over (partition by user_id order by log_t desc) as rn
        from parse_actionlog where ds='{date}' and account in (select distinct account from parse_nginx where ds='{date}' and appid='cnwnioswnhd' and method='new_user')) t1 where t1.rn=1
    '''.format(date=date)
    print last_sql
    last_df = hql_to_df(last_sql)

    liushi_sql = '''
        select user_id from parse_info where ds='{date}' and regexp_replace(to_date(reg_time), '-', '')='{date}' and account not in
        (select distinct account from parse_info where ds='{date_1}')
    '''.format(date=date, date_1=ds_add(date, 1))
    print liushi_sql
    liushi_df = hql_to_df(liushi_sql)

    result = last_df[last_df['user_id'].isin(liushi_df['user_id'])]
    return result

if __name__ == '__main__':
    settings_dev.set_env('dancer_pub')
    result_list = []
    for date in date_range('20170401', '20170406'):
        # tmp_20170410_last_action(date).to_excel(r'E:\Data\output\dancer\last_action_%s.xlsx'%date)
        result_list.append(tmp_20170410_last_action(date))
    result = pd.concat(result_list)
    result.to_excel(r'E:\Data\output\dancer\last_action.xlsx')
