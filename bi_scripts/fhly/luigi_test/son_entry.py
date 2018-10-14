#!/usr/bin/env python
# -*- coding: UTF-8 -*-
import luigi
import datetime
import os
import settings_dev
from settings_dev import hive_path
from lib.utils import load_data_to_file
from lib.utils import error_log, DateFormat, Parse_data, ColorPrint
from luigi_tools import parse_account, parse_login,parse_role


class ParseEntry(luigi.Task):
    '''解析文件
    '''
    date = luigi.DateParameter(
        default=datetime.date.today() - datetime.timedelta(days=1))
    platform = luigi.Parameter()
    task_name = luigi.Parameter()

    def run(self):
        message = '>>>start run {0} {1}'.format(self.platform, self.task_name)
        ColorPrint(message)
        rawfile_name = self.task_name.replace('parse', 'raw')
        date_str = self.date.strftime('%Y%m%d')
        raw_path = settings_dev.raw_table_path[rawfile_name].format(
            date=date_str)
        if os.path.exists(raw_path):
            file_in = os.path.join(
                os.path.dirname(raw_path), os.path.basename(raw_path))
            file_out_name = 'parse_' + os.path.basename(raw_path)
            file_out = os.path.join(os.path.dirname(raw_path), file_out_name)
            # 加入解析文件是否存在的判断
            # print file_out, file_out_name
            if not os.path.exists(file_out):
                if self.task_name == 'parse_account':
                    Parse_data(file_in, file_out, parse_account)
                elif self.task_name == 'parse_login':
                    Parse_data(file_in, file_out, parse_login)
                elif self.task_name == 'parse_role':
                    Parse_data(file_in, file_out, parse_role)
                print '{task_name} parse complete!'.format(
                    task_name=self.task_name)
            else:
                print '{task_name} have exist!'.format(
                    task_name=self.task_name)
            hive_url_path = hive_path.format(db=self.platform,
                                             table=self.task_name,
                                             date_str=date_str,
                                             filename=file_out_name)
            load_data_to_file(hive_url_path, file_out, self.platform,
                              self.task_name, date_str, file_out_name)
        else:
            error_log('{raw_path} not in local'.format(raw_path=raw_path),
                      DateFormat(date_str))
            raise NameError(
                '{raw_path} not in local'.format(raw_path=raw_path),
                DateFormat(date_str))
        self.output().open('w').close()

    def output(self):
        history_path = os.path.join(settings_dev.BASE_ROOT,
                                    'history_task_test', self.platform,
                                    str(self.date), self.task_name)
        return luigi.LocalTarget(history_path)


