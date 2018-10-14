#!/usr/bin/env python
# -*- coding: UTF-8 -*-
import os
import luigi
import datetime
import settings_dev
from settings_dev import hive_path

from mid_hive_table import mid_dic, dancer_bt_mid_dic
from mid_hive_table import dancer_mul_mid_dic, dancer_pub_mid_dic

from lib.utils import load_data_to_file
from lib.utils import run_hql
from lib.utils import ds_add
from lib.utils import error_log
from lib.utils import DateFormat
from lib.utils import Parse_data
from lib.utils import ColorPrint
from lib.utils import check_exist

from luigi_tools import parse_actionlog_all
from luigi_tools import parse_nginx
from luigi_tools import parse_info
from luigi_tools import mul_parse_info, pub_parse_info,bt_parse_info
from luigi_tools import parse_sdk_nginx
from luigi_tools import parse_voided_data


class RawEntry(luigi.Task):
    '''将原始数据从27导入hive
    '''
    date = luigi.DateParameter(
        default=datetime.date.today() - datetime.timedelta(days=1))
    platform = luigi.Parameter()
    task_name = luigi.Parameter()

    def run(self):
        message = '>>>start run {0} {1}'.format(self.platform, self.task_name)
        ColorPrint(message)
        date_str = self.date.strftime('%Y%m%d')
        local_path = settings_dev.raw_table_path[self.task_name].format(
            date=date_str)
        filename = os.path.basename(local_path)
        hive_url_path = hive_path.format(db=self.platform,
                                         table=self.task_name,
                                         date_str=date_str,
                                         filename=filename)
        load_data_to_file(hive_url_path, local_path, self.platform,
                          self.task_name, date_str)
        self.output().open('w').close()

    def output(self):
        history_path = os.path.join(settings_dev.BASE_ROOT,
                                    'history_task_test', self.platform,
                                    str(self.date), self.task_name)
        return luigi.LocalTarget(history_path)


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
            print file_out
            # 加入解析文件是否存在的判断
            if not os.path.exists(file_out):
                if self.task_name == 'parse_actionlog':
                    Parse_data(file_in, file_out, parse_actionlog_all)
                elif self.task_name == 'parse_nginx':
                    Parse_data(file_in, file_out, parse_nginx)
                elif self.task_name == 'parse_sdk_nginx':
                    Parse_data(file_in, file_out, parse_sdk_nginx)
                elif self.task_name == 'parse_voided_data':
                    Parse_data(file_in, file_out, parse_voided_data)
                elif self.task_name == 'parse_info':
                    if self.platform == 'dancer_mul':
                        # 多语言版本的info解析与其他版本不一致
                        Parse_data(file_in, file_out, mul_parse_info)
                    elif self.platform == 'dancer_kr':
                        # 多语言版本的info解析与其他版本不一致
                        Parse_data(file_in, file_out, mul_parse_info)
                    elif self.platform in ['dancer_pub']:
                        # 国服多神器系统
                        Parse_data(file_in, file_out, pub_parse_info)
                    elif self.platform in ['dancer_bt', 'dancer_cgwx']:
                        # 国服多神器系统
                        Parse_data(file_in, file_out, bt_parse_info)
                    else:
                        Parse_data(file_in, file_out, parse_info)
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


class MidHqlEntry(luigi.Task):
    '''通过hql将数据导入hive
    '''
    date = luigi.DateParameter(
        default=datetime.date.today() - datetime.timedelta(days=1))
    platform = luigi.Parameter()
    task_name = luigi.Parameter()

    def run(self):
        message = '>>>start run {0} {1}'.format(self.platform, self.task_name)
        ColorPrint(message)
        try:
            date_str = self.date.strftime('%Y%m%d')
            yestoday = ds_add(date_str, -1)
            if not check_exist(self.platform, self.task_name, date_str):
                if self.task_name in settings_dev.independent_list:
                    if self.platform == 'dancer_mul':
                        # 多语言版本的info解析与其他版本不一致
                        hql = dancer_mul_mid_dic[self.task_name].format(
                            date=date_str, yestoday=yestoday)
                    elif self.platform == 'dancer_kr':
                        # 多语言版本的info解析与其他版本不一致
                        hql = dancer_mul_mid_dic[self.task_name].format(
                            date=date_str, yestoday=yestoday)
                    elif self.platform in ['dancer_pub']:
                        hql = dancer_pub_mid_dic[self.task_name].format(
                            date=date_str, yestoday=yestoday)
                    elif self.platform in ['dancer_bt', 'dancer_cgame', 'dancer_cgwx']:
                        hql = dancer_bt_mid_dic[self.task_name].format(
                            date=date_str, yestoday=yestoday)
                    else:
                        hql = mid_dic[self.task_name].format(date=date_str,
                                                             yestoday=yestoday)
                    run_hql(hql, db=self.platform)
                elif (settings_dev.start_date == self.date) or check_exist(
                        self.platform, self.task_name, yestoday):
                    if self.platform == 'dancer_mul':
                        # 多语言版本的info解析与其他版本不一致
                        hql = dancer_mul_mid_dic[self.task_name].format(
                            date=date_str, yestoday=yestoday)
                    elif self.platform == 'dancer_kr':
                        # 多语言版本的info解析与其他版本不一致
                        hql = dancer_mul_mid_dic[self.task_name].format(
                            date=date_str, yestoday=yestoday)
                    elif self.platform in ['dancer_pub']:
                        hql = dancer_pub_mid_dic[self.task_name].format(
                            date=date_str, yestoday=yestoday)
                    elif self.platform in ['dancer_bt', 'dancer_cgame', 'dancer_cgwx']:
                        hql = dancer_bt_mid_dic[self.task_name].format(
                            date=date_str, yestoday=yestoday)
                    else:
                        hql = mid_dic[self.task_name].format(date=date_str,
                                                             yestoday=yestoday)
                    run_hql(hql, db=self.platform)
                else:
                    error_message = '{yestoday}.{platform}.{task_name} Not In Hive'.format(
                        task_name=self.task_name,
                        yestoday=yestoday,
                        platform=self.platform)
                    error_log(error_message, DateFormat(date_str))
                    raise Exception(error_message, DateFormat(date_str))
            else:
                error_log('{date_str}.{task_name} Already In Hive'.format(
                    task_name=self.task_name,
                    date_str=date_str),
                          DateFormat(date_str))
        except TypeError:
            print 'mid hql error'
        self.output().open('w').close()

    def output(self):
        history_path = os.path.join(settings_dev.BASE_ROOT,
                                    'history_task_test', self.platform,
                                    str(self.date), self.task_name)
        return luigi.LocalTarget(history_path)


class MartEntry(luigi.Task):
    '''跑mart脚本
    '''
    date = luigi.DateParameter(
        default=datetime.date.today() - datetime.timedelta(days=1))
    platform = luigi.Parameter()
    task_name = luigi.Parameter()

    def run(self):
        settings_dev.set_env(self.platform)
        message = '>>>start run {0} {1}'.format(self.platform, self.task_name)
        ColorPrint(message)
        date_str = self.date.strftime('%Y%m%d')
        filename = self.task_name + '_' + date_str
        file_dir = os.path.join(settings_dev.local_data_dir, 'mart', filename)
        if not os.path.exists(file_dir):
            job_func = getattr(
                __import__('{code_dir}.mart.{task_name}'.format(
                    code_dir=settings_dev.code_dir,
                    task_name=self.task_name),
                           globals(),
                           locals(), [self.task_name]),
                self.task_name)
            result_df = job_func(date_str)
            result_df.to_csv(file_dir, sep='\t', index=False, header=False)
        else:
            print '{filename} is Exist!'.format(filename=filename)
        hive_url_path = hive_path.format(db=self.platform,
                                         table=self.task_name,
                                         date_str=date_str,
                                         filename=filename)
        load_data_to_file(hive_url_path, file_dir, self.platform,
                          self.task_name, date_str, filename)
        self.output().open('w').close()

    def output(self):
        history_path = os.path.join(settings_dev.BASE_ROOT,
                                    'history_task_test', self.platform,
                                    str(self.date), self.task_name)
        return luigi.LocalTarget(history_path)


class DisEntry(luigi.Task):
    '''跑dis脚本
    '''
    date = luigi.DateParameter(
        default=datetime.date.today() - datetime.timedelta(days=1))
    platform = luigi.Parameter()
    task_name = luigi.Parameter()

    def run(self):
        settings_dev.set_env(self.platform)
        message = '>>>start run {0} {1}'.format(self.platform, self.task_name)
        ColorPrint(message)
        job_func = getattr(
            __import__('{code_dir}.display.{task_name}'.format(
                code_dir=settings_dev.code_dir,
                task_name=self.task_name),
                       globals(),
                       locals(), [self.task_name]),
            self.task_name)
        job_func(self.date.strftime('%Y%m%d'))
        self.output().open('w').close()

    def output(self):
        history_path = os.path.join(settings_dev.BASE_ROOT,
                                    'history_task_test', self.platform,
                                    str(self.date), self.task_name)
        return luigi.LocalTarget(history_path)
