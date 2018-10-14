#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Dong Junshuang
Description : 检查数据
按照顺序输入日期和平台即可查询hive中是否存在指定日期的数据
'''
from lib.utils import check_hive_data
from settings_dev import hive_path
import settings_dev
import os
import sys


def check_data(platform, job, date_str, filename, name):
    if job.startswith(name):
        hive_dtl_path = hive_path.format(db=platform,
                                         table=job,
                                         date_str=date_str,
                                         filename=filename)
        if not check_hive_data(hive_dtl_path, date_str):
            print '{platform}.{job} is not in hive!'.format(platform=platform,
                                                            job=job)


if __name__ == '__main__':
    date_str = sys.argv[1]
    platform = sys.argv[2]
    # platform = 'qiling_ks_bak'
    settings_dev.set_env(platform)
    # date_str = '20170109'
    for job in settings_dev.job_deps.keys():
        if job.startswith('raw_'):
            filename = os.path.basename(settings_dev.raw_table_path[job].format(
                date=date_str))
            check_data(platform, job, date_str, filename, 'raw_')
        else:
            check_data(platform, job, date_str, job + '_' + date_str, 'mart_')
            check_data(platform, job, date_str, '000000_0', 'mid_')

    print 'check out!'
