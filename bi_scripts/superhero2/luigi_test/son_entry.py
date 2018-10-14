#!/usr/bin/env python
# -*- coding: UTF-8 -*-
import luigi
import datetime
import os
import settings_dev
from settings_dev import hive_path
from lib.utils import load_data_to_file, run_hql, ds_add, check_hive_data
from lib.utils import error_log, DateFormat, Parse_data, ColorPrint
from mid_hive_table import mid_dic, foreign_mid_dic
from luigi_tools import parse_action_log, parse_nginx, parse_nginx_1, parse_info, parse_stones, parse_item, parse_equip, parse_hero, parse_act_user, parse_new_user
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
            if self.task_name == 'parse_action_log':
                # 加入解析文件是否存在的判断
                # print file_out, file_out_name
                if not os.path.exists(file_out):
                    Parse_data(file_in, file_out, parse_action_log)
                    print 'parse_action_log parse complete!'
                else:
                    print 'parse_action_log have exist!'
                hive_url_path = hive_path.format(db=self.platform,
                                                 table=self.task_name,
                                                 date_str=date_str,
                                                 filename=file_out_name)
                load_data_to_file(hive_url_path, file_out, self.platform,
                                  self.task_name, date_str, file_out_name)
            if self.task_name == 'parse_nginx':
                # 加入解析文件是否存在的判断
                # print file_out, file_out_name
                if not os.path.exists(file_out):
                    Parse_data(file_in, file_out, parse_nginx)
                    print 'parse_nginx parse complete!'
                else:
                    print 'parse_nginx have exist!'
                hive_url_path = hive_path.format(db=self.platform,
                                                 table=self.task_name,
                                                 date_str=date_str,
                                                 filename=file_out_name)
                load_data_to_file(hive_url_path, file_out, self.platform,
                                  self.task_name, date_str, file_out_name)
                #添加
            if self.task_name == 'parse_nginx_1':
                # 加入解析文件是否存在的判断
                # print file_out, file_out_name
                if not os.path.exists(file_out):
                    Parse_data(file_in, file_out, parse_nginx_1)
                    print 'parse_nginx_1 parse complete!'
                else:
                    print 'parse_nginx_1 have exist!'
                hive_url_path = hive_path.format(db=self.platform,
                                                 table=self.task_name,
                                                 date_str=date_str,
                                                 filename=file_out_name)
                load_data_to_file(hive_url_path, file_out, self.platform,
                                  self.task_name, date_str, file_out_name)




            if self.task_name == 'parse_info':
                # 加入解析文件是否存在的判断
                # print file_out, file_out_name
                if not os.path.exists(file_out):
                    Parse_data(file_in, file_out, parse_info)
                    print 'parse_info parse complete!'
                else:
                    print 'parse_info have exist!'
                hive_url_path = hive_path.format(db=self.platform,
                                                 table=self.task_name,
                                                 date_str=date_str,
                                                 filename=file_out_name)
                load_data_to_file(hive_url_path, file_out, self.platform,
                                  self.task_name, date_str, file_out_name)
            if self.task_name == 'parse_hero':
                # 加入解析文件是否存在的判断
                # print file_out, file_out_name
                if not os.path.exists(file_out):
                    Parse_data(file_in, file_out, parse_hero)
                    print 'parse_hero parse complete!'
                else:
                    print 'parse_hero have exist!'
                hive_url_path = hive_path.format(db=self.platform,
                                                 table=self.task_name,
                                                 date_str=date_str,
                                                 filename=file_out_name)
                load_data_to_file(hive_url_path, file_out, self.platform,
                                  self.task_name, date_str, file_out_name)
            if self.task_name == 'parse_equip':
                # 加入解析文件是否存在的判断
                # print file_out, file_out_name
                if not os.path.exists(file_out):
                    Parse_data(file_in, file_out, parse_equip)
                    print 'parse_equip parse complete!'
                else:
                    print 'parse_equip have exist!'
                hive_url_path = hive_path.format(db=self.platform,
                                                 table=self.task_name,
                                                 date_str=date_str,
                                                 filename=file_out_name)
                load_data_to_file(hive_url_path, file_out, self.platform,
                                  self.task_name, date_str, file_out_name)
            if self.task_name == 'parse_item':
                # 加入解析文件是否存在的判断
                # print file_out, file_out_name
                if not os.path.exists(file_out):
                    Parse_data(file_in, file_out, parse_item)
                    print 'parse_item parse complete!'
                else:
                    print 'parse_item have exist!'
                hive_url_path = hive_path.format(db=self.platform,
                                                 table=self.task_name,
                                                 date_str=date_str,
                                                 filename=file_out_name)
                load_data_to_file(hive_url_path, file_out, self.platform,
                                  self.task_name, date_str, file_out_name)
            if self.task_name == 'parse_stones':
                # 加入解析文件是否存在的判断
                # print file_out, file_out_name
                if not os.path.exists(file_out):
                    Parse_data(file_in, file_out, parse_stones)
                    print 'parse_stones parse complete!'
                else:
                    print 'parse_stones have exist!'
                hive_url_path = hive_path.format(db=self.platform,
                                                 table=self.task_name,
                                                 date_str=date_str,
                                                 filename=file_out_name)
                load_data_to_file(hive_url_path, file_out, self.platform,
                                  self.task_name, date_str, file_out_name)
            if self.task_name == 'parse_act_user':
                # 加入解析文件是否存在的判断
                # print file_out, file_out_name
                if not os.path.exists(file_out):
                    Parse_data(file_in, file_out, parse_act_user)
                    print 'parse_act_user parse complete!'
                else:
                    print 'parse_act_user have exist!'
                hive_url_path = hive_path.format(db=self.platform,
                                                 table=self.task_name,
                                                 date_str=date_str,
                                                 filename=file_out_name)
                load_data_to_file(hive_url_path, file_out, self.platform,
                                  self.task_name, date_str, file_out_name)
            if self.task_name == 'parse_new_user':
                # 加入解析文件是否存在的判断
                # print file_out, file_out_name
                if not os.path.exists(file_out):
                    Parse_data(file_in, file_out, parse_new_user)
                    print 'parse_new_user parse complete!'
                else:
                    print 'parse_new_user have exist!'
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
            hive_dtl_path = hive_path.format(db=self.platform,
                                             table=self.task_name,
                                             date_str=date_str,
                                             filename='000000_0')
            if not check_hive_data(hive_dtl_path, date_str):
                yes_hive_path = hive_path.format(db=self.platform,
                                                 table=self.task_name,
                                                 date_str=yestoday,
                                                 filename='000000_0')
                if (settings_dev.start_date == self.date) or check_hive_data(
                        yes_hive_path, yestoday):
                    if self.platform in ['superhero2_tw', 'superhero2']:
                        hql = mid_dic[self.task_name].format(date=date_str,
                                                             yestoday=yestoday)
                        run_hql(hql, db=self.platform)
                    else:
                        hql = foreign_mid_dic[self.task_name].format(
                            date=date_str, yestoday=yestoday)
                        run_hql(hql, db=self.platform)
                else:
                    error_log(
                        '{yestoday}.{platform}.{task_name} not in hive and {today}.{platform}.{task_name} not load to hive'.format(
                            task_name=self.task_name,
                            yestoday=yestoday,
                            today=date_str,
                            platform=self.platform),
                        DateFormat(date_str))
                    raise NameError('{yestoday}.{platform}.{task_name} not in hive and {today}.{platform}.{task_name} not load to hive'.format(
                            task_name=self.task_name,
                            yestoday=yestoday,
                            today=date_str,
                            platform=self.platform),
                        DateFormat(date_str))
            else:
                error_log('{date_str}.{task_name} already in hive'.format(
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
