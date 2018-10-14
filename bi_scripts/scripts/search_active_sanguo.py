#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
===武娘版本===
"""
import os
import shutil
import time
import datetime

import settings_dev
from utils import date_range, get_active_conf, ds_add


def sanguo_active_info(db, ds):
    settings_dev.set_env(db)
    active_dict = {
        'super_rich': u'宇宙最强',
        'hero_version': u'限时传奇卡',
        # 'oracle_reward': u'神域活动/无量宝藏',
        'large_super_rich': u'跨服宇宙最强',
        'foundation_new': u'卧龙基金',
        'gringotts': u'古灵阁',
        'raiders': u'讨伐黄巾贼',
        # 'foundation': u'超英福利基金',
    }
    for keys in active_dict:
        try:
            active_name = keys.strip()
            version, start_time, end_time = get_active_conf(active_name, ds)
            if version != '':
                content = u'开启活动'
                print ds, db, content, active_name, active_dict.get(active_name)
        except Exception, e:
            pass


def del_cache_file(db):
    try:
        shutil.rmtree(os.path.abspath('config/{0}/').format(db))
        print 'del {0} cache file complete'.format(db)
    except Exception, e:
        print("\033[1;31;40m{0}\033[0m").format(e)
        pass


if __name__ == '__main__':
    weeks_now = int(time.strftime('%w'))
    if weeks_now == 4:
        today = str(datetime.date.today()).replace('-', '')
    date_now = ds_add(today, -1)
    date_early = ds_add(today, -7)
    print u'查询日期区间:', date_now, date_early
    dbcount = ['sanguo_tw', 'sanguo_ks', 'sanguo_tl']
    for db in dbcount:
        del_cache_file(db)
        for ds in date_range(date_early, date_now):
            sanguo_active_info(db, ds)
