#!/usr/bin/env python
# -*- coding: UTF-8 -*-
import luigi
import datetime
import os
import settings_dev
# import pandas as pd
from settings_dev import hive_path
from lib.utils import load_data_to_file, run_hql, ds_add
from lib.utils import error_log, DateFormat, Parse_data, ColorPrint
from mid_hive_table import mid_dic, foreign_mid_dic, self_mid_dic, bi_mid_dic,mul_mid_dic,vt_mid_dic
from luigi_tools import parse_actionlog, parse_nginx
from lib.utils import check_exist
import pandas as pd

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
        rawfile_name = self.task_name.replace('parse', 'raw')
        date_str = self.date.strftime('%Y%m%d')
        raw_path = settings_dev.raw_table_path[rawfile_name].format(
            date=date_str)
        if os.path.exists(raw_path):
            file_in = os.path.join(
                os.path.dirname(raw_path), os.path.basename(raw_path))
            file_out_name = 'parse_' + os.path.basename(raw_path)
            file_out = os.path.join(os.path.dirname(raw_path), file_out_name)
            if self.task_name == 'parse_actionlog':
                # 加入解析文件是否存在的判断
                # print file_out, file_out_name
                if not os.path.exists(file_out):
                    Parse_data(file_in, file_out, parse_actionlog)
                    print 'parse_actionlog parse complete!'
                else:
                    print 'parse_actionlog have exist!'
                hive_url_path = hive_path.format(db=self.platform,
                                                 table='mid_actionlog',
                                                 date_str=date_str,
                                                 filename=file_out_name)
                load_data_to_file(hive_url_path, file_out, self.platform,
                                  'mid_actionlog', date_str, file_out_name)
            if self.task_name == 'parse_nginx':
                # 加入解析文件是否存在的判断
                # print file_out, file_out_name
                if not os.path.exists(file_out):
                    Parse_data(file_in, file_out, parse_nginx)
                    print 'parse_nginx parse complete!'
                else:
                    print 'parse_nginx have exist!'
                hive_url_path = hive_path.format(db=self.platform,
                                                 table='parse_nginx',
                                                 date_str=date_str,
                                                 filename=file_out_name)
                load_data_to_file(hive_url_path, file_out, self.platform,
                                  'parse_nginx', date_str, file_out_name)
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
                    if self.platform in ['superhero_qiku',
                                         'superhero_tw']:
                        hql = mid_dic[self.task_name].format(date=date_str,
                                                             yestoday=yestoday)
                        run_hql(hql, db=self.platform)
                    elif self.platform == 'superhero_bi':
                        hql = bi_mid_dic[self.task_name].format(
                            date=date_str, yestoday=yestoday)
                        run_hql(hql, db=self.platform)
                    elif self.platform == 'superhero_mul':
                        hql = mul_mid_dic[self.task_name].format(
                            date=date_str, yestoday=yestoday)
                        run_hql(hql, db=self.platform)
                    elif self.platform == 'superhero_vt':
                        hql = vt_mid_dic[self.task_name].format(
                            date=date_str, yestoday=yestoday)
                        run_hql(hql, db=self.platform)
                    elif self.platform == 'superhero_self_en':
                        hql = self_mid_dic[self.task_name].format(
                            date=date_str, yestoday=yestoday)
                        run_hql(hql, db=self.platform)
                    else:
                        hql = foreign_mid_dic[self.task_name].format(
                            date=date_str, yestoday=yestoday)
                        run_hql(hql, db=self.platform)
                elif (settings_dev.start_date == self.date) or check_exist(
                        self.platform, self.task_name, yestoday):
                    if self.platform in ['superhero_qiku',
                                         'superhero_tw']:
                        hql = mid_dic[self.task_name].format(date=date_str,
                                                             yestoday=yestoday)
                        run_hql(hql, db=self.platform)
                    elif self.platform == 'superhero_bi':
                        hql = bi_mid_dic[self.task_name].format(
                            date=date_str, yestoday=yestoday)
                        run_hql(hql, db=self.platform)
                    elif self.platform == 'superhero_mul':
                        hql = mul_mid_dic[self.task_name].format(
                            date=date_str, yestoday=yestoday)
                        run_hql(hql, db=self.platform)
                    elif self.platform == 'superhero_vt':
                        hql = vt_mid_dic[self.task_name].format(
                            date=date_str, yestoday=yestoday)
                        run_hql(hql, db=self.platform)
                    elif self.platform == 'superhero_self_en':
                        hql = self_mid_dic[self.task_name].format(
                            date=date_str, yestoday=yestoday)
                        run_hql(hql, db=self.platform)
                    else:
                        hql = foreign_mid_dic[self.task_name].format(
                            date=date_str, yestoday=yestoday)
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
        except Exception:
            print 'Mid Hql Error'
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
        # 判断本地文件是否存在
        if not os.path.exists(file_dir):
            # 判断任务是否依赖前一天的数据
            # if self.task_name not in settings_dev.dependent_list:
            job_func = getattr(
                __import__('{code_dir}.mart.{task_name}'.format(
                    code_dir=settings_dev.code_dir,
                    task_name=self.task_name),
                           globals(),
                           locals(), [self.task_name]),
                self.task_name)
            result_df = job_func(date_str)
            result_df.to_csv(file_dir, sep='\t', index=False, header=False)
            # else:
            #     yestoday = ds_add(date_str, -1)
            #     # 判断hdfs文件是否存在
            #     if (settings_dev.start_date == self.date) or check_exist(
            #             self.platform, self.task_name, yestoday):
            #         job_func = getattr(
            #             __import__('{code_dir}.mart.{task_name}'.format(
            #                 code_dir=settings_dev.code_dir,
            #                 task_name=self.task_name),
            #                        globals(),
            #                        locals(), [self.task_name]),
            #             self.task_name)
            #         result_df = job_func(date_str)
            #         result_df.to_csv(file_dir,sep='\t',index=False,header=False)
            #
            #     else:
            #         error_message = '{yestoday}.{platform}.{task_name} Not In Hive'.format(
            #             task_name=self.task_name,
            #             yestoday=yestoday,
            #             platform=self.platform)
            #         error_log(error_message, DateFormat(date_str))
            #         raise Exception(error_message, DateFormat(date_str))
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

# 用于补特定的任务
class SuperheroDailyDemo(luigi.Task):
    '''解析文件
    '''
    date = luigi.DateParameter(
        default=datetime.date.today() - datetime.timedelta(days=1))
    platform = luigi.Parameter()
    job = luigi.Parameter()

    def run(self):
        rawfile_name = self.job.replace('parse', 'raw')
        date_str = self.date.strftime('%Y%m%d')
        platform = self.platform
        # raw_path = settings_dev.raw_table_path[rawfile_name].format(date=date_str)
        if self.platform == 'superhero_bi':
            platform = 'superhero'
        if self.platform == 'superhero_vt':
            platform = 'superhero_vietnam'
        raw_path = '/home/data/{platform}/nginx_log/access.log_{date}'.format(platform=platform,date=date_str)
        print os.path.exists(raw_path)
        if os.path.exists(raw_path):
            file_in = os.path.join(
                os.path.dirname(raw_path), os.path.basename(raw_path))
            file_out_name = 'parse_' + os.path.basename(raw_path)
            file_out = os.path.join(os.path.dirname(raw_path), file_out_name)
            if self.job == 'parse_nginx':
                # 加入解析文件是否存在的判断
                if not os.path.exists(file_out):
                    Parse_data(file_in, file_out, parse_nginx)
                    print 'parse_nginx parse complete!'
                else:
                    print 'parse_nginx have exist!'
                hive_url_path = hive_path.format(db=self.platform,
                                                 table='parse_nginx',
                                                 date_str=date_str,
                                                 filename=file_out_name)

                load_data_to_file(hive_url_path, file_out, self.platform,
                                  'parse_nginx', date_str, file_out_name)

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
                                    str(self.date), self.job)
        return luigi.LocalTarget(history_path)

