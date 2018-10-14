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
from mid_hive_table import GenAllHiveMidTable

class PythonTask(luigi.Task):
    '''通过制定日期和脚本名执行python任务
    '''
    # resources = {'hive': 1}
    date = luigi.DateParameter()
    job = luigi.Parameter()
    platform = luigi.Parameter()

    # def requires(self):
    #     return GenAllHiveMidTable(date=self.date, platform=self.platform)

    def run(self):
        self._run()
        self.output().open('w').close()

    def output(self):
        history_path = os.path.join(settings.BASE_ROOT, 'history',
                                    self.platform, str(self.date),
                                    self.job + '_' + str(self.date))
        return luigi.LocalTarget(history_path)

    def _run(self):
        settings.set_env(self.platform)
        job_func = getattr(
            __import__('{code_dir}.display.{job}'.format(
                code_dir=settings.code_dir,
                job=self.job),
                       globals(),
                       locals(),
                       [self.job]),
            self.job)
        job_func(self.date.strftime('%Y%m%d'))


class PythonJobs(sl.WorkflowTask):
    '''执行某个平台一天的所有python任务'''
    date = luigi.DateParameter(
        default=datetime.date.today() - datetime.timedelta(days=1))
    platform = luigi.Parameter()

    def workflow(self):
        # 导入所有数据到hive
        tasks = []
        settings.set_env(self.platform)

        # python_job_list = [f[:-3] for f in os.listdir(os.path.join(settings.BASE_ROOT, 'superhero2', 'display')) if f.startswith('dis_') and f.endswith('.py')]
        python_job_list = [
            # 'dis_act_3day',
            # 'dis_new_big_r_info',
            # 'dis_chain',
            # 'dis_equip',
            # 'dis_gongping_rob',
            # 'dis_official_lv_distr',
            # 'dis_pay_platform',
            # 'dis_spirit_info',
        ]
        # print python_job_list
        for job in python_job_list:
            pythonWorkflow = sl.new_task('python_job',
                                         PythonTask,
                                         self,
                                         date=self.date,
                                         job=job,
                                         platform=self.platform)
            tasks.append(pythonWorkflow)
        return tasks


if __name__ == '__main__':
    sl.run(main_task_cls=PythonJobs,
                 cmdline_args=['--platform=superhero2', '--date=2016-04-24'])
