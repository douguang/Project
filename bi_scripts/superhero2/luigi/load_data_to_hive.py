#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Dong Junshuang
Description :
'''
import os
import luigi
import sciluigi as sl
import datetime
import settings
from utils import load_file_to_hive, run_hql
from luigi.contrib.hdfs.target import HdfsTarget

class RsyncTask(sl.Task):
    ''' 一个封装好的rsync任务
    '''
    src = luigi.Parameter()
    dst = luigi.Parameter()

    def run(self):
        self.ex('''/usr/bin/rsync -avz --partial-dir=.rsync-partial {src} {dst}
        '''.format(**{
            'src': self.src,
            'dst': self.dst,
        }))

    def out_data(self):
        return sl.TargetInfo(self, self.dst)


class LoadDataToHiveTask(sl.Task):
    '''载入文件到表'''
    localfile = luigi.Parameter()
    table = luigi.Parameter()
    db = luigi.Parameter()
    partition = luigi.Parameter(default='')

    in_upstream = None

    def run(self):
        load_file_to_hive(self.localfile,
                          self.table,
                          self.db,
                          self.partition,
                          check=False)

    def output(self):
        partition = self.partition or self.localfile[-8:]
        # print 'LoadDataToHiveTask', partition, self.localfile, type(self.partition)
        hdfs_out_path = '/user/hive/warehouse/{db}.db/{table}/ds={partition}/{filename}'.format(
            **{
                'db': self.db,
                'partition': partition,
                'table': self.table,
                'filename': os.path.basename(self.localfile)
            })
        # print hdfs_out_path
        return HdfsTarget(hdfs_out_path)


class LoadHiveDataWorkflow(sl.WorkflowTask):
    '''从rsync到载入hive的workflow'''
    localfile_src = luigi.Parameter()
    localfile = luigi.Parameter()
    table = luigi.Parameter()
    db = luigi.Parameter()
    partition = luigi.Parameter(default='')

    def workflow(self):
        rsync = self.new_task('rsync',
                              RsyncTask,
                              dst=self.localfile,
                              src=self.localfile_src)

        load_files_to_hive = self.new_task('load_data_to_hive',
                                           LoadDataToHiveTask,
                                           localfile=self.localfile,
                                           table=self.table,
                                           db=self.db,
                                           partition=self.partition)
        load_files_to_hive.in_upstream = rsync.out_data

        return load_files_to_hive

class PlatformDataToHive(sl.WorkflowTask):
    '''将一个平台一天的所有原始数据导入hive'''
    date = luigi.DateParameter(
        default=datetime.date.today() - datetime.timedelta(days=1))
    platform = luigi.Parameter()

    def workflow(self):
        tasks = []
        settings.set_env(self.platform)
        from settings import yield_remotefile_localfile_table_map
        for remote_path, local_path, table in yield_remotefile_localfile_table_map(
                self.date.strftime('%Y%m%d')):
            hiveWorkflow = sl.new_task(table,
                                       LoadHiveDataWorkflow,
                                       self,
                                       localfile_src=remote_path,
                                       localfile=local_path,
                                       table=table,
                                       db=settings.platform)
            tasks.append(hiveWorkflow)
        return tasks


class RunAllDataToHive(sl.WorkflowTask):
    def workflow(self):
        # 导入所有数据到hive
        tasks = []

        platform = 'superhero2'
        for date in range(24, 25):
            task = self.new_task('daily_task',
                                 PlatformDataToHive,
                                 platform=platform,
                                 date=datetime.date(2016, 4, date))
            tasks.append(task)
        return tasks


if __name__ == '__main__':
    sl.run_local(main_task_cls=RunAllDataToHive)
