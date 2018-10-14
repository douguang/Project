#!/usr/bin/env python
# -*- coding: UTF-8 -*-
import luigi
import datetime
import time
import os
import sciluigi as sl
import settings_dev
from son_entry import MartEntry, MidHqlEntry, RawEntry, DisEntry, ParseEntry


class TaskTrigger(sl.WorkflowTask):
    date = luigi.DateParameter(
        default=datetime.date.today() - datetime.timedelta(days=1))
    platform = luigi.Parameter()
    task_name = luigi.Parameter()

    def workflow(self):
        tasks = []
        settings_dev.set_env(self.platform)
        if self.task_name.startswith('raw_'):
            tasks.append(self.new_task('RawEntry',
                                       RawEntry,
                                       platform=self.platform,
                                       task_name=self.task_name,
                                       date=self.date))
        elif self.task_name.startswith('mid_'):
            tasks.append(self.new_task('MidHqlEntry',
                                       MidHqlEntry,
                                       platform=self.platform,
                                       task_name=self.task_name,
                                       date=self.date))
        elif self.task_name.startswith('mart_'):
            tasks.append(self.new_task('MartEntry',
                                       MartEntry,
                                       platform=self.platform,
                                       task_name=self.task_name,
                                       date=self.date))
        elif self.task_name.startswith('dis_'):
            tasks.append(self.new_task('DisEntry',
                                       DisEntry,
                                       platform=self.platform,
                                       task_name=self.task_name,
                                       date=self.date))
        elif self.task_name.startswith('parse_'):
            tasks.append(self.new_task('ParseEntry',
                                       ParseEntry,
                                       platform=self.platform,
                                       task_name=self.task_name,
                                       date=self.date))
        return tasks


class UniformEntry(luigi.Task):
    '''包含依赖关系的统一入口'''
    date = luigi.DateParameter(
        default=datetime.date.today() - datetime.timedelta(days=1))
    platform = luigi.Parameter()
    task_name = luigi.Parameter()

    def requires(self):
        return [UniformEntry(date=self.date,
                             platform=self.platform,
                             task_name=dep_name)
                for dep_name in settings_dev.job_deps.get(self.task_name, [])]

    def run(self):
        yield TaskTrigger(date=self.date,
                          platform=self.platform,
                          task_name=self.task_name)
        self.output().open('w').close()

    def output(self):
        history_path = os.path.join(settings_dev.BASE_ROOT,
                                    'history_task_test', self.platform,
                                    str(self.date), self.task_name)
        return luigi.LocalTarget(history_path)


class QilingDaily(sl.WorkflowTask):
    '''
    执行入口
    '''
    date = luigi.DateParameter(
        default=datetime.date.today() - datetime.timedelta(days=1))
    platform = luigi.Parameter()

    def workflow(self):
        settings_dev.set_env(self.platform)
        tasks = []
        # 跑所有表
        history_path = os.path.join(settings_dev.BASE_ROOT,
                                    'history_task_test', self.platform,
                                    str(self.date))
        if os.path.exists(history_path) == False:
            for job_deps in settings_dev.job_deps:
                tasks.append(self.new_task('UniformEntry',
                                           UniformEntry,
                                           platform=self.platform,
                                           task_name=job_deps,
                                           date=self.date))
        else:
            job_list = []
            for all_job in os.listdir(history_path):
                job_list.append(all_job)
            for job_deps in settings_dev.job_deps:
                if job_deps not in job_list:
                    tasks.append(self.new_task('UniformEntry',
                                               UniformEntry,
                                               platform=self.platform,
                                               task_name=job_deps,
                                               date=self.date))
        return tasks


class QilingDailyRaw(sl.WorkflowTask):
    '''
    执行入口
    '''
    date = luigi.DateParameter(
        default=datetime.date.today() - datetime.timedelta(days=1))
    platform = luigi.Parameter()

    def workflow(self):
        settings_dev.set_env(self.platform)
        tasks = []
        # 跑所有表
        for job_deps in settings_dev.job_deps:
            if job_deps.startswith('raw_'):
                tasks.append(self.new_task('UniformEntry',
                                           UniformEntry,
                                           platform=self.platform,
                                           task_name=job_deps,
                                           date=self.date))
        return tasks


# 临时用，之后补Mart数据
class QilingDailyMart(sl.WorkflowTask):
    '''
    执行入口
    '''
    date = luigi.DateParameter(
        default=datetime.date.today() - datetime.timedelta(days=1))
    platform = luigi.Parameter()

    def workflow(self):
        settings_dev.set_env(self.platform)
        tasks = []
        # 跑所有表
        for job_deps in settings_dev.job_deps:
            if job_deps.startswith('mid_assist'):
                tasks.append(self.new_task('UniformEntry',
                                           UniformEntry,
                                           platform=self.platform,
                                           task_name=job_deps,
                                           date=self.date))
        return tasks
