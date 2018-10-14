#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Dong Junshuang
Description : 各个luigi任务的统一入口
'''
import settings
import luigi
import datetime
import os
import sciluigi as sl
from luigi_tools import parse_actionlog
from mid_hive_table import hql_table_depend_dic, GetHiveMidTable
from load_data_to_hive import RsyncTask, LoadDataToHiveTask
from python_job import PythonTask


class ParseActionlog(sl.Task):
    '''解析json格式行为日志成 tsv 格式'''
    in_data = None

    def out_data(self):
        out_path = os.path.join(os.path.dirname(self.in_data().path), 'parsed_' + os.path.basename(self.in_data().path))
        return sl.TargetInfo(self, out_path)

    def run(self):
        with self.in_data().open() as f_in, self.out_data().open('w') as f_out:
            for line in f_in:
                try:
                    line_out = parse_actionlog(line)
                    f_out.write(line_out)
                except:
                    print line

class TaskTrigger(sl.WorkflowTask):
    date = luigi.DateParameter(
        default=datetime.date.today() - datetime.timedelta(days=1))
    platform = luigi.Parameter()
    task_name = luigi.Parameter()

    def workflow(self):
        settings.set_env(self.platform)
        date_str = self.date.strftime('%Y%m%d')
        tasks = []
        if self.task_name == 'rsync_actionlog':
            # mid_actionlog 和 raw_actionlog 会同时拉数据，独立出拉数据任务，作为那两个任务的依赖
            local_path = settings.raw_table_path['raw_actionlog']['local'].format(date=date_str)
            remote_path = settings.raw_table_path['raw_actionlog']['remote'].format(date=date_str)
            rsync = self.new_task('RsyncTask',
                                  RsyncTask,
                                  dst=local_path,
                                  src=remote_path)
            tasks.append(rsync)
        elif self.task_name == 'mid_actionlog':
            # 行为日志先解析成tsv再上传到mid_actionlog
            local_path = settings.raw_table_path['raw_actionlog']['local'].format(date=date_str)
            remote_path = settings.raw_table_path['raw_actionlog']['remote'].format(date=date_str)
            rsync = self.new_task('RsyncTask',
                                  RsyncTask,
                                  dst=local_path,
                                  src=remote_path)
            parse_actionlog_task = self.new_task('parse_actionlog', ParseActionlog)
            parse_actionlog_task.in_data = rsync.out_data
            load_files_to_hive = self.new_task('LoadDataToHiveTask',
                                               LoadDataToHiveTask,
                                               localfile=parse_actionlog_task.out_data().path,
                                               table=self.task_name,
                                               db=settings.platform)
            load_files_to_hive.in_upstream = parse_actionlog_task.out_data
            tasks.append(load_files_to_hive)
        # 原始表的上传
        elif self.task_name.startswith('raw_'):
            local_path = settings.raw_table_path[self.task_name]['local'].format(date=date_str)
            remote_path = settings.raw_table_path[self.task_name]['remote'].format(date=date_str)
            rsync = self.new_task('RsyncTask',
                                  RsyncTask,
                                  dst=local_path,
                                  src=remote_path)
            load_files_to_hive = self.new_task('LoadDataToHiveTask',
                                               LoadDataToHiveTask,
                                               localfile=local_path,
                                               table=self.task_name,
                                               db=settings.platform)
            load_files_to_hive.in_upstream = rsync.out_data
            tasks.append(load_files_to_hive)
        # 生成中间表
        elif self.task_name.startswith('mid_'):
            hql = hql_table_depend_dic[self.task_name][0]
            depend_on_yestoday = hql_table_depend_dic[self.task_name][1]
            hql_task = self.new_task('GetHiveMidTable',
                                     GetHiveMidTable,
                                     date=self.date,
                                     platform=self.platform,
                                     depend_on_yestoday=depend_on_yestoday,
                                     hql=hql,
                                     table=self.task_name)
            tasks.append(hql_task)
        # 执行python脚本
        elif self.task_name.startswith('dis_'):
            pythonWorkflow = sl.new_task('PythonTask',
                                         PythonTask,
                                         self,
                                         date=self.date,
                                         job=self.task_name,
                                         platform=self.platform)
            tasks.append(pythonWorkflow)
        return tasks

# 任务依赖关系
job_deps = {
    # 'raw_action_log': [],
    # 'raw_paylog': [],
    # 'raw_spendlog': [],
    # 'raw_reg': [],
    # 'raw_act': [],
    # 'raw_card': [],
    # 'raw_equip': [],
    # 'raw_item': [],
    # 'raw_info': [],
    # 'dis_card_attend': ['raw_action_log'],
    # 'raw_card_attend': ['dis_card_attend'],
    'dis_single_card_use_rate': ['raw_card_attend'],
    'raw_card_use_rate': ['dis_single_card_use_rate'],
    # 'mid_info_all':['raw_info'],
    # 'mid_paylog_all':['raw_paylog'],
    # 'mid_new_account': ['raw_reg','raw_info','mid_info_all'],
    # 'dis_reg_user_ltv':['raw_info','raw_paylog','mid_info_all'], # 注册用户LTV(account)
    # 'dis_gift_pay_detail':['raw_paylog'], # 礼包购买详情
    # 'dis_day_coin_spend':['mid_info_all','raw_spendlog','raw_paylog'], # 每日钻石消费
    # 'dis_daily_data':['raw_reg','raw_info','mid_paylog_all','raw_paylog'], # 日常数据
    # 'dis_card_evo':['raw_card'], # 卡牌进阶
    # 'dis_card_relive':['raw_card'], # 卡牌转生
    # 'dis_single_card_use_rate':['raw_card'], # 卡牌使用率
    # 'dis_keep_rate':['raw_info','raw_reg','mid_info_all'], # 留存率
    # 'dis_spend_detail':['mid_info_all','raw_spendlog','mid_paylog_all'], # 消费详情、钻石商城、限购商城
    # 'dis_card_equip_quality':['raw_equip'], # 命运装备品质
    # 'dis_card_equip_num':['raw_equip'], # 命运装备数量
}

class UniformEntry(luigi.Task):
    '''包含依赖关系的统一入口'''
    date = luigi.DateParameter(
        default=datetime.date.today() - datetime.timedelta(days=1))
    platform = luigi.Parameter()
    task_name = luigi.Parameter()

    def requires(self):
        return [UniformEntry(date=self.date, platform=self.platform, task_name=dep_name) for dep_name in job_deps.get(self.task_name, [])]

    def run(self):
        yield TaskTrigger(date=self.date, platform=self.platform, task_name=self.task_name)
        self.output().open('w').close()

    def output(self):
        history_path = os.path.join(settings.BASE_ROOT, 'history_task',
                                    self.platform, str(self.date),
                                    self.task_name)
        return luigi.LocalTarget(history_path)

class DailyTrigger(sl.WorkflowTask):
    date = luigi.DateParameter(
        default=datetime.date.today() - datetime.timedelta(days=1))

    def workflow(self):
        tasks = []
        platform = 'superhero2'
        for task_name in job_deps:
            tasks.append(self.new_task('DailyTrigger', UniformEntry, platform=platform, task_name=task_name, date=self.date))
        return tasks

if __name__ == '__main__':
    sl.run(main_task_cls=DailyTrigger)
