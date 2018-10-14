#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Dong Junshuang
Description :
'''
import luigi
import datetime
import sciluigi as sl
from python_job import PythonJobs
import settings


class RunAllTasks(sl.WorkflowTask):
    '''执行某个平台一天的所有任务'''
    date = luigi.DateParameter(
        default=datetime.date.today() - datetime.timedelta(days=1))

    def workflow(self):
        tasks = []
        platform = 'superhero2'
        settings.set_env(platform)
        platformWorkflow = self.new_task('python_job',
                                         PythonJobs,
                                         date=self.date,
                                         platform=platform)
        tasks.append(platformWorkflow)
        return tasks


if __name__ == '__main__':
    sl.run(main_task_cls=RunAllTasks)
