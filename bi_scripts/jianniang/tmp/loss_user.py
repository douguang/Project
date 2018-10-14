#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
流失用户相关脚本

次留数据

流失用户等级（战力、繁荣度）分布

留存用户等级（战力、繁荣度）分布

流失用户最后动作停留

流失用户最后关卡停留

流失用户在线时长分布
"""
import settings_dev
from utils import hql_to_df, format_date, ds_add


def loss_user(ds, del_ds):
    fds = format_date(ds_add(ds, int(del_ds)))
    print fds
    sql = '''
    select user_id
    from raw_info
    where ds = '{0}'
    and to_date(reg_time) = '{1}'
    '''.format(ds, fds)
    df = hql_to_df(sql)
    print df.head()


def loss_user_info(ds):
    fds = format_date(ds_add(ds, -1))
    print fds
    sql = '''
    select user_id, sword, raid, task_guide
    from
    (
        select user_id, score
        from raw_sword
        where ds = '{0}'
    ) t1
    left outer join
    (
        select user_id, score
        from raw_develop_rank
        where ds = '{0}'
    ) t2 on t1.user_id = t2.user_id
    '''.format(ds, fds)
    df = hql_to_df(sql)


if __name__ == '__main__':
    settings_dev.set_env('jianniang_test')
    ds = '20170319'
    loss_user(ds)
