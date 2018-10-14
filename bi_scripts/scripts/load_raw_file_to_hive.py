#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Dong Junshuang
Description : 批量上传原始文件到hive（可指定日期区间和文件）
Time        : 2017.03.23
'''
import os
import settings_dev
from settings_dev import hive_path
from lib.utils import load_data_to_file
from lib.utils import ds_add
from lib.utils import ColorPrint
from utils import date_range

if __name__ == '__main__':
    platform = 'superhero_qiku'
    task_name = 'raw_talisman'
    settings_dev.set_env(platform)

    for date_str in date_range('20170315', '20170331'):
        message = '>>>start run {0} {1}'.format(platform, task_name)
        ColorPrint(message)
        local_path = settings_dev.raw_table_path[task_name].format(date=date_str)
        filename = os.path.basename(local_path)
        hive_url_path = hive_path.format(db=platform,
                                         table=task_name,
                                         date_str=date_str,
                                         filename=filename)
        load_data_to_file(hive_url_path, local_path, platform, task_name, date_str)
