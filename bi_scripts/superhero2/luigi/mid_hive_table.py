#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Dong Junshuang
Description :
'''
import luigi
import sciluigi as sl
import datetime
import settings
from luigi.contrib.hdfs.target import HdfsTarget
from utils import run_hql
from load_data_to_hive import PlatformDataToHive


class GetHiveMidTable(luigi.Task):
    date = luigi.DateParameter()
    hql = luigi.Parameter()
    table = luigi.Parameter()
    platform = luigi.Parameter()
    depend_on_yestoday = luigi.BoolParameter(default=False)

    def requires(self):
        # yield PlatformDataToHive(date=self.date, platform=self.platform)
        if self.depend_on_yestoday and self.date > settings.start_date:
            yield GetHiveMidTable(
                date=self.date - datetime.timedelta(days=1),
                hql=self.hql,
                table=self.table,
                platform=self.platform,
                depend_on_yestoday=self.depend_on_yestoday
            )

    def run(self):
        hql = self.hql.format(
            table=self.table,
            date=self.date.strftime('%Y%m%d'),
            yestoday=(self.date - datetime.timedelta(days=1)).strftime('%Y%m%d'),
        )
        print hql
        run_hql(hql, db=self.platform)

    def output(self):
        hdfs_out_path = '/user/hive/warehouse/{db}.db/{table}/ds={partition}/000000_0'.format(
            db=self.platform,
            table=self.table,
            partition=self.date.strftime('%Y%m%d')
        )
        print hdfs_out_path
        return HdfsTarget(hdfs_out_path)

hql_info_all = '''
insert overwrite table {table}
partition (ds='{date}')
select uid,account,nick,platform_2,device,create_time,fresh_time,vip_level,level,zuanshi,gold_coin,silver_coin,only_access,file_date
from (
    select *,
           row_number() over(partition by uid order by ds desc) as rn
    from
    (
        select * from raw_info where ds = '{date}'
        union all
        select * from {table} where ds = '{yestoday}'
    ) t1
) t2 where rn = 1
'''

table_info_all = 'mid_info_all'

hql_paylog_all = '''
insert overwrite table {table}
partition (ds='{date}')
select order_id,level,old_coin,gift_coin,order_coin,order_money,order_rmb,is_double,money_type,order_time,platform_2,conf_id,platconf_id,platorder_id,uid,plat_id,admin,reason
from (
    select *,
           row_number() over(partition by order_id order by ds desc) as rn
    from
    (
        select * from raw_paylog where ds = '{date}'
        union all
        select * from {table} where ds = '{yestoday}'
    ) t1
) t2 where rn = 1
'''

table_paylog_all = 'mid_paylog_all'

hql_new_account = '''
insert overwrite table {table}
partition (ds='{date}')
SELECT a.account,
       a.uid,
       a.plat,
       a.platform_2,
       a.server
FROM
  (SELECT account,
          uid,
          substr(uid,1,1) plat,
          platform_2,
          reverse(substr(reverse(uid), 8)) AS server
   FROM raw_info
   WHERE ds= '{date}')a LEFT semi
JOIN
  (SELECT uid
   FROM raw_reg
   WHERE ds= '{date}')b ON a.uid=b.uid
WHERE a.account NOT IN
    (SELECT account
     FROM mid_info_all
     WHERE ds= '{yestoday}')
'''
table_new_account = 'mid_new_account'

hql_table_depend_list = (
    (hql_paylog_all, table_paylog_all, True),
    (hql_info_all, table_info_all, True),
    (hql_new_account, table_new_account, True),
)

hql_table_depend_dic = {
    'mid_paylog_all': (hql_paylog_all, True),
    'mid_info_all': (hql_info_all, True),
    'mid_new_account': (hql_new_account, True),
}

class GenAllHiveMidTable(sl.WorkflowTask):
    '''生成一天所有的中间表'''
    date = luigi.DateParameter(
        default=datetime.date.today() - datetime.timedelta(days=1))
    platform = luigi.Parameter()

    def workflow(self):
        tasks = []
        settings.set_env(self.platform)
        for hql, table, depend_on_yestoday in hql_table_depend_list:
            hql_task = sl.new_task(table,
                                   GetHiveMidTable,
                                   self,
                                   date=self.date,
                                   platform=self.platform,
                                   depend_on_yestoday=depend_on_yestoday,
                                   hql=hql,
                                   table=table)
            tasks.append(hql_task)
        return tasks


if __name__ == '__main__':
    sl.run_local(main_task_cls=GenAllHiveMidTable,
                 cmdline_args=['--platform=superhero2', '--date=2016-04-24'])
