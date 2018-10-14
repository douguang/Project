#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Hu Wenchao
Description : 国战官职等级分布
'''
import settings_dev
from utils import hql_to_df, update_mysql, ds_add


def dis_official_lv_distr(date):
    table = 'dis_official_lv_distr'

    official_lv_sql = '''
    select '{date}' as ds,
           vip,
           reverse(substring(reverse(user_id), 8)) as server,
           count(user_id) as act_3d,
           sum(case when official_lv=61 then 1 else 0 end) as id_61,
           sum(case when official_lv=60 then 1 else 0 end) as id_60,
           sum(case when official_lv=59 then 1 else 0 end) as id_59,
           sum(case when official_lv=58 then 1 else 0 end) as id_58,
           sum(case when official_lv=57 then 1 else 0 end) as id_57,
           sum(case when official_lv=56 then 1 else 0 end) as id_56,
           sum(case when official_lv=55 then 1 else 0 end) as id_55,
           sum(case when official_lv=54 then 1 else 0 end) as id_54,
           sum(case when official_lv=53 then 1 else 0 end) as id_53,
           sum(case when official_lv=52 then 1 else 0 end) as id_52,
           sum(case when official_lv=51 then 1 else 0 end) as id_51,
           sum(case when official_lv=50 then 1 else 0 end) as id_50,
           sum(case when official_lv=49 then 1 else 0 end) as id_49,
           sum(case when official_lv=48 then 1 else 0 end) as id_48,
           sum(case when official_lv=47 then 1 else 0 end) as id_47,
           sum(case when official_lv=46 then 1 else 0 end) as id_46,
           sum(case when official_lv=45 then 1 else 0 end) as id_45,
           sum(case when official_lv=44 then 1 else 0 end) as id_44,
           sum(case when official_lv=43 then 1 else 0 end) as id_43,
           sum(case when official_lv=42 then 1 else 0 end) as id_42,
           sum(case when official_lv=41 then 1 else 0 end) as id_41,
           sum(case when official_lv=40 then 1 else 0 end) as id_40,
           sum(case when official_lv=39 then 1 else 0 end) as id_39,
           sum(case when official_lv=38 then 1 else 0 end) as id_38,
           sum(case when official_lv=37 then 1 else 0 end) as id_37,
           sum(case when official_lv=36 then 1 else 0 end) as id_36,
           sum(case when official_lv=35 then 1 else 0 end) as id_35,
           sum(case when official_lv=34 then 1 else 0 end) as id_34,
           sum(case when official_lv=33 then 1 else 0 end) as id_33,
           sum(case when official_lv=32 then 1 else 0 end) as id_32,
           sum(case when official_lv=31 then 1 else 0 end) as id_31,
           sum(case when official_lv=30 then 1 else 0 end) as id_30,
           sum(case when official_lv=29 then 1 else 0 end) as id_29,
           sum(case when official_lv=28 then 1 else 0 end) as id_28,
           sum(case when official_lv=27 then 1 else 0 end) as id_27,
           sum(case when official_lv=26 then 1 else 0 end) as id_26,
           sum(case when official_lv=25 then 1 else 0 end) as id_25,
           sum(case when official_lv=24 then 1 else 0 end) as id_24,
           sum(case when official_lv=23 then 1 else 0 end) as id_23,
           sum(case when official_lv=22 then 1 else 0 end) as id_22,
           sum(case when official_lv=21 then 1 else 0 end) as id_21,
           sum(case when official_lv=20 then 1 else 0 end) as id_20,
           sum(case when official_lv=19 then 1 else 0 end) as id_19,
           sum(case when official_lv=18 then 1 else 0 end) as id_18,
           sum(case when official_lv=17 then 1 else 0 end) as id_17,
           sum(case when official_lv=16 then 1 else 0 end) as id_16,
           sum(case when official_lv=15 then 1 else 0 end) as id_15,
           sum(case when official_lv=14 then 1 else 0 end) as id_14,
           sum(case when official_lv=13 then 1 else 0 end) as id_13,
           sum(case when official_lv=12 then 1 else 0 end) as id_12,
           sum(case when official_lv=11 then 1 else 0 end) as id_11,
           sum(case when official_lv=10 then 1 else 0 end) as id_10,
           sum(case when official_lv=9 then 1 else 0 end) as id_9,
           sum(case when official_lv=8 then 1 else 0 end) as id_8,
           sum(case when official_lv=7 then 1 else 0 end) as id_7,
           sum(case when official_lv=6 then 1 else 0 end) as id_6,
           sum(case when official_lv=5 then 1 else 0 end) as id_5,
           sum(case when official_lv=4 then 1 else 0 end) as id_4,
           sum(case when official_lv=3 then 1 else 0 end) as id_3,
           sum(case when official_lv=2 then 1 else 0 end) as id_2,
           sum(case when official_lv=1 then 1 else 0 end) as id_1
    from
    (
        select user_id, vip, official_lv
        from mid_info_all
        where ds = '{date}'
    ) t1
    left semi join
    (
        select user_id
        from raw_activeuser
        where ds <= '{date}' and ds >= '{date_in_3days}'
    ) t2 on t1.user_id = t2.user_id
    group by vip, server
    '''.format(**{
        'date': date,
        'date_in_3days': ds_add(date, -2),
    })

    print official_lv_sql
    official_lv_df = hql_to_df(official_lv_sql)
    print official_lv_df

    # 更新MySQL表
    del_sql = 'delete from {0} where ds="{1}"'.format(table, date)
    update_mysql(table, official_lv_df, del_sql)


if __name__ == '__main__':
    settings_dev.set_env('metal_test')
    dis_official_lv_distr('20160422')
