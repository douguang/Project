#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Dong Junshuang
Description : 新增uid用户等级分布
'''
import settings_dev
from utils import hql_to_df, update_mysql, ds_add, date_range
from pandas import DataFrame
from jinja2 import Template

days = [8, 15, 31]


def dis_common_uid_level_ondate(date):
    '''跑某天的数据
    '''
    # 用模板构造 sql
    info_sql_template = '''
    select ds,
           {% for day in days %}
           d{{ day }}.level as d{{ day }}_level,
           {% endfor %}
           new.user_id as user_id
    from
    (
        select ds, user_id
        from raw_registeruser
        where ds = '{{ date }}'
    ) new
    {% for day in days %}
    left outer join
    (
        select user_id, level
        from mid_info_all
        where ds = '{{ ds_add(date, day-1) }}'
    ) d{{ day }} on d{{ day }}.user_id = new.user_id
    {% endfor %}
    '''

    d = {
        'days': days,    # 要求的N日等级列表
        'ds_add': ds_add,
        'date': date,
    }
    info_sql = Template(info_sql_template).render(**d)
    print info_sql
    info_df = hql_to_df(info_sql)
    info_df = info_df.fillna(0)
    print info_df

    # MySQL中保存原始数据表
    table = 'dis_common_uid_level_raw'
    del_sql = 'delete from {0} where ds = "{1}"'.format(table, date)
    update_mysql(table, info_df, del_sql)

    # 新用户等级分布
    level_distr = {
        'ds': date,
        'new_num': info_df.user_id.count(),
    }
    for day in d['days']:
        level_distr['d%d_lv_mean' % day] = info_df['d%d_level' % day].mean()
        level_distr['d%d_lv_median' % day] = info_df[
            'd%d_level' % day].median()

    uid_lv_distr_df = DataFrame({k: [v] for k, v in level_distr.iteritems()})

    table = 'dis_common_uid_level'
    print table
    del_sql = 'delete from {0} where ds = "{1}"'.format(table, date)
    update_mysql(table, uid_lv_distr_df, del_sql)


def dis_common_uid_level(date):
    '''某天跑，更新会影响的日期数据
    '''
    server_start_date = settings_dev.start_date.strftime('%Y%m%d')
    run_dates = filter(lambda d: d >= server_start_date, [
                       ds_add(date, 1 - d) for d in days])
    for f_date in run_dates:
        dis_common_uid_level_ondate(f_date)

if __name__ == '__main__':
    for platform in ['sanguo_tw', 'sanguo_ks', 'sanguo_kr']:
        settings_dev.set_env(platform)
        for date in date_range('20161110', '20161121'):
            dis_common_uid_level(date)
