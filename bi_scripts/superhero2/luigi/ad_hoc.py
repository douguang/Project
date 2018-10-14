#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Dong Junshuang
Description : 临时执行的各种脚本，例如补数据
'''

import luigi
import datetime
import sciluigi as sl
from python_job import PythonJobs
import settings
from daily_trigger import RunAllTasks
from mid_hive_table import GenAllHiveMidTable

class AdHoc(sl.WorkflowTask):
    def workflow(self):
        tasks = []
        today = datetime.date.today()
        date = datetime.date(2016, 4, 19)
        while date < today:
            platformWorkflow = self.new_task('runall', RunAllTasks, date=date)
            tasks.append(platformWorkflow)
            date += datetime.timedelta(days=1)
        return tasks

class GetAllHistMidTable(sl.WorkflowTask):
    '''从中间表步骤开始补开服至今的'''
    def workflow(self):
        tasks = []
        today = datetime.date.today()
        date = datetime.date(2016, 4, 19)
        while date < today:
            for platform in ['superhero2']:
                platformWorkflow = self.new_task('GetAllHistMidTable', GenAllHiveMidTable, date=date, platform=platform)
                tasks.append(platformWorkflow)
            date += datetime.timedelta(days=1)
        return tasks



if __name__ == '__main__':
    sl.run(main_task_cls=AdHoc)
