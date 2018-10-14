#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Hu Wenchao
Description : 常用辅助函数
'''
import os
import sys
import time
import urllib
import random
import string
import json
import datetime
import hashlib
import pandas as pd
import numpy as np
from hdfs import InsecureClient
from multiprocessing.dummy import Pool
import sqlalchemy

import settings_dev
from settings_dev import mysql_template, hive_template, impala_template


def format_date(date, format_type='YYYY-MM-DD'):
    ''' 格式化时间

    >>> format_date('20160301')
    1
    >>> '2016-03-01'
    1
    '''
    format_type = format_type.replace('YYYY', date[0:4])
    format_type = format_type.replace('MM', date[4:6])
    format_type = format_type.replace('DD', date[-2:])
    return format_type


def format_stamp(timestamp):
    ''' 格式化时间戳

    >>> format_stamp('1488193427')
    1
    >>> '2017-02-27'
    1
    '''
    # return datetime.datetime.utcfromtimestamp(timestamp).strftime("%Y-%m-%d") # UTC时间
    return datetime.datetime.fromtimestamp(timestamp).strftime("%Y-%m-%d") # 北京时间


def ds_delta(start, end, date_format='%Y%m%d'):
    ''' 日期相减
    - delta: 间隔天数

    >>> ds_delta('20160301', '20160302')
    1
    >>> ds_add('2016-03-01', '2016-03-02', '%Y-%m-%d')
    1
    '''
    now = datetime.datetime.strptime(start, date_format)
    end = datetime.datetime.strptime(end, date_format)
    if now <= end:
        delta = (end - now).days
        return delta


def ds_add(date, delta, date_format='%Y%m%d'):
    ''' 日期计算
    - delta: 间隔天数，可为负数

    >>> ds_add('20160301', 2)
    '20160303'
    >>> ds_add('20160301', -2)
    '20160228'
    >>> ds_add('2016-03-01', -2, '%Y-%m-%d')
    '2016-02-28'
    '''
    return datetime.datetime.strftime(
        datetime.datetime.strptime(date, date_format) +
        datetime.timedelta(delta), date_format)


def update_mysql(table, df, pre_sql=None, db=None):
    '''用dataframe更新MySQL表
    - table: 表名
    - df: 要插入的dataframe
    - pre_sql: 插入数据前要执行的SQL操作，用来删除当天的数据，避免数据重复插入
    '''
    db = db or settings_dev.platform
    mysql_url = mysql_template.format(db=db)
    engine = sqlalchemy.create_engine(mysql_url, encoding="utf-8")
    conn_mysql = engine.raw_connection()
    try:
        cur = conn_mysql.cursor()
        if pre_sql:
            # 先检查表是否存在，如果不存在，则不执行 pre_sql
            cur.execute("show tables like '{0}'".format(table))
            if cur.fetchone():
                cur.execute(pre_sql)
        # 参考：http://stackoverflow.com/questions/30631325/writing-to-mysql-database-with-pandas-using-sqlalchemy-to-sql
        df.to_sql(con=engine, name=table, if_exists='append', index=False)
    finally:
        conn_mysql.close()


def hql_to_df(hql, server='impala', db=None):
    '''将hql检索出结果放入dataframe中返回，默认使用impala，速度更快。
    >>> hql_to_df('show tables like "raw_info"').name[0]
    'raw_info'
    '''
    db = db or settings_dev.platform
    if server == 'impala':
        url = impala_template.format(db=db)
    elif server == 'hive':
        url = hive_template.format(db=db)
    else:
        raise Exception('argument server have to be hive or impala!')
    # print url
    engine = sqlalchemy.create_engine(url)
    conn = engine.raw_connection()
    try:
        if server == 'impala':
            cur = conn.cursor()
            cur.execute('INVALIDATE METADATA')
        df = pd.read_sql(hql, conn)
    finally:
        conn.close()
    return df


def hqls_to_dfs(hqls, server='impala', db=None):
    '''并行处理多个hql请求
    >>> sqls = ['show tables like "{0}"'.format(i) for i in ['raw_info', 'raw_paylog']]
    >>> [df.name[0] for df in hqls_to_dfs(sqls)]
    ['raw_info', 'raw_paylog']
    '''
    assert isinstance(hqls, list)
    pool = Pool(5)
    results = pool.map(lambda x: hql_to_df(x, server, db), hqls)
    pool.close()
    pool.join()
    return results


def run_hql(hql, db='', server='hive'):
    '''直接执行一些无返回值
    >>> run_hql('show databases; show tables; create database if not exists test_database;', 'sanguo')
    '''
    if server == 'impala':
        url = impala_template.format(db=db)
    elif server == 'hive':
        url = hive_template.format(db=db)
    else:
        raise Exception('argument server have to be hive or impala!')
    engine = sqlalchemy.create_engine(url)
    conn = engine.raw_connection()
    hqls = [s.strip() for s in hql.split(';') if s.strip()]
    try:
        cur = conn.cursor()
        for one_hql in hqls:
            print one_hql
            cur.execute(one_hql)
    finally:
        conn.close()


def load_files_to_hive(file_table_db_par_list, check=True):
    '''将本地文件载入到hive表
    '''
    try:
        hdfs_client = InsecureClient(settings_dev.hdfs_url)
        engine = sqlalchemy.create_engine(hive_template.format(db=''))
        conn_hive = engine.raw_connection()
        try:
            cur = conn_hive.cursor()
            for i in file_table_db_par_list:
                # print i
                if len(i) == 3:
                    local_path, table, db = i
                    partition = local_path[-8:]
                elif len(i) == 4:
                    local_path, table, db, partition = i
                    partition = partition or local_path[-8:]
                else:
                    raise Exception('argument %s format is wrong!' % i)

                print local_path, table, db, partition
                filename = os.path.basename(local_path)
                hdfs_path = '/tmp/{db}/{filename}'.format(**{
                    'filename': filename,
                    'db': db
                })
                if check:
                    hive_path = '/user/hive/warehouse/{db}.db/{table}/ds={partition}/{filename}'.format(
                        db=db,
                        table=table,
                        filename=filename,
                        partition=partition)
                    if hdfs_client.status(hive_path, strict=False):
                        print '{local_path} already in hive!'.format(
                            local_path=local_path)
                        continue

                remote_path = hdfs_client.upload(hdfs_path,
                                                 local_path,
                                                 overwrite=True)
                if not remote_path:
                    remote_path = hdfs_client.upload(hdfs_path,
                                                     local_path,
                                                     overwrite=True)
                    if not remote_path:
                        print '{local_path} upload failed'
                        sys.exit()

                hql = '''
                load data inpath '{remote_path}'
                into table {db}.{table}
                partition (ds='{partition}')
                '''.format(**{
                    'partition': partition,
                    'remote_path': remote_path,
                    'table': table,
                    'db': db,
                })
                # print hql
                cur.execute(hql)
                print 'Success: {local_path} -> {db}.{table} partition: {partition}\n'.format(
                    **{
                        'table': table,
                        'db': db,
                        'local_path': local_path,
                        'partition': partition
                    })
        finally:
            conn_hive.close()
    except Exception, e:
        print e


def load_file_to_hive(local_path, table, db, partition=None, check=True):
    '''将本地文件载入到hive表
    '''
    try:
        hdfs_client = InsecureClient(settings_dev.hdfs_url)
        engine = sqlalchemy.create_engine(hive_template.format(db=''))
        conn_hive = engine.raw_connection()
        try:
            cur = conn_hive.cursor()
            partition = partition or local_path[-8:]
            # print local_path, table, db, partition
            filename = os.path.basename(local_path)
            hdfs_path = '/tmp/{db}/{filename}'.format(**{
                'filename': filename,
                'db': db
            })
            if check:
                hive_path = '/user/hive/warehouse/{db}.db/{table}/ds={partition}/{filename}'.format(
                    db=db,
                    table=table,
                    filename=filename,
                    partition=partition)
                if hdfs_client.status(hive_path, strict=False):
                    print '{local_path} already in hive!'.format(
                        local_path=local_path)
                    return

            if os.path.exists(local_path):
                i = 5
                while i > 0:
                    remote_path = hdfs_client.upload(hdfs_path,
                                                     local_path,
                                                     overwrite=True)
                    if remote_path:
                        i = i - 5
                    else:
                        i = i - 1
                if not remote_path:
                    print '{local_path} upload failed\n'.format(
                        **{'local_path': local_path})
                    sys.exit()
            else:
                sys.exit()

            hql = '''
            load data inpath '{remote_path}'
            into table {db}.{table}
            partition (ds='{partition}')
            '''.format(**{
                'partition': partition,
                'remote_path': remote_path,
                'table': table,
                'db': db,
            })
            # print hql
            cur.execute(hql)
            print 'Success: {local_path} -> {db}.{table} partition: {partition}\n'.format(
                **{
                    'table': table,
                    'db': db,
                    'local_path': local_path,
                    'partition': partition
                })
        finally:
            conn_hive.close()
    except Exception, e:
        print e


def download_config(config_name):
    '''下载配置到当前文件夹'''
    dir_path = os.path.join(settings_dev.BASE_ROOT, 'config',
                            settings_dev.platform)
    if not os.path.exists(dir_path):
        os.makedirs(dir_path)

    URL = settings_dev.url
    data = {'url': URL, 'config_name': config_name}
    config_url = '{url}/config/?method=all_config&config_name={config_name}'.format(
        **data)
    # print config_url
    text = urllib.urlopen(config_url).read()
    d = json.loads(text)
    config_path = os.path.join(dir_path, config_name)
    with open(config_path, 'w') as f:
        json.dump(d['data'][config_name], f, indent=4)


def get_server_list():
    URL = settings_dev.url
    server_list_url = '{url}/config/?method=server_list'.format(url=URL)
    server_list = json.loads(urllib.urlopen(server_list_url).read())
    return {server_dic['server']: server_dic['open_time']
            for server_dic in server_list['data']['server_list']}


def get_server_days(date):
    """
    获取服务器列表和开服时间
    date: 查询日期20170101
    """
    server_list = get_server_list()
    for i in server_list:
        i = i.strip()
        server_list[i] = format_stamp(server_list[i])
    server_df = pd.DataFrame(server_list.items(),
                             columns=[
                                 'server', 'open_date'
                             ])
    server_df['ds'] = pd.to_datetime(format_date(date))
    server_df['open_date'] = server_df.open_date.astype('datetime64')
    server_df['days'] = (
        (server_df.ds - server_df.open_date) / np.timedelta64(1, 'D') + 1
    ).astype('int')
    return server_df[['server', 'open_date', 'days']]


def get_config(config_name):
    '''返回obj格式的配置'''
    dir_path = os.path.join(settings_dev.BASE_ROOT, 'config',
                            settings_dev.platform)
    config_path = os.path.join(dir_path, config_name)
    if not os.path.isfile(config_path):
        download_config(config_name)
    with open(config_path) as f:
        return json.load(f)


def timestamp_to_string(tp, t_format='%Y-%m-%d %H:%M:%S'):
    '''
    >>> timestamp_to_string(0)
    1970-01-01 08:00:00
    '''
    return datetime.datetime.fromtimestamp(tp).strftime(t_format)


def string_to_timestamp(t_str, t_format='%Y-%m-%d %H:%M:%S'):
    '''
    >>> string_to_timestamp('1970-01-01 08:00:00')
    0.0
    '''
    return time.mktime(datetime.datetime.strptime(t_str, t_format).timetuple())


def date_range(start, end, date_format='%Y%m%d'):
    '''
    >>> date_range('20160301', '20160302')
    ['20160301', '20160302']
    '''
    # result = []
    now = datetime.datetime.strptime(start, date_format)
    end = datetime.datetime.strptime(end, date_format)
    while now <= end:
        # result.append(now.strftime(date_format))
        yield now.strftime(date_format)
        now += datetime.timedelta(days=1)
    # return result


def multi_pt_sql(hql, platforms):
    '''查询多个平台的数据，合并结果
    '''
    dfs = []
    origin_pt = settings_dev.platform
    for pt in platforms:
        settings_dev.set_env(pt)
        dfs.append(hql_to_df(hql))
    settings_dev.set_env(origin_pt)
    return pd.concat(dfs)


def format_dates(dates):
    '''具有兼容性的传入SQL的日期列表，如当使用
       select * from table where ds in {dates}
       时，可以用该方法格式化dates

    >>> format_dates(['20160101'])
    ('20160101')
    >>> format_dates(['20160101', '20160102'])
    ('20160101','20160102')
    '''
    return '({0})'.format(','.join(repr(str(s)) for s in dates))


def get_active_conf(conf_name, date):
    """
    获取活动的配置文件
    将date = '20161215' 转化为date = '2016-12-15 00:00:00'
    """
    conf = get_config(conf_name)
    # print conf
    # conf_list = []
    version, act_start_time, act_end_time = '', '', ''
    dt = date[:4] + '-' + date[4:6] + '-' + date[6:8] + ' 00:00:01'
    for key in conf.keys():
        start_time = conf['%s' % key]['start_time']
        end_time = conf['%s' % key]['end_time']
        if ('/' in start_time) or ('/' in end_time):
            start_time = start_time.replace('/', '-')
            end_time = end_time.replace('/', '-')
        if dt >= start_time and dt <= end_time:
            version, act_start_time, act_end_time = key, start_time, end_time
    return version, act_start_time, act_end_time


def get_server_active_conf(conf_name, date, server_days):
    """
    获取新服活动的配置文件
    'conf_name': 活动名，server_开头
    'date': 脚本执行日期
    'server_days': 开服第N日
    将date = '20161215' 转化为date = '2016-12-15 00:00:00'
    返回version, act_days(开服第N日), act_start_time, act_end_time
    """
    conf = get_config(conf_name)
    version, act_days, act_start_time, act_end_time = '', '', '', ''
    for key in conf.keys():
        act_day = str(conf['%s' % key]['start_time']).split(' ')[0]
        if act_day == server_days:
            dt = date[:4] + '-' + date[4:6] + '-' + date[6:8]
            start_time = dt + str(conf['%s' % key]['start_time'])[-9:]
            end_time = dt + str(conf['%s' % key]['end_time'])[-9:]
            version, act_days, act_start_time, act_end_time = key, act_day, start_time, end_time
    return version, act_days, act_start_time, act_end_time


def ds_short(date):
    '''
    格式化日期
    将日期：'2016-12-20' 转化为'20161220'
    '''
    return date[0:4] + date[5:7] + date[8:10]


def ds_len(date):
    '''
    格式化日期
    将日期：'20161220' 转化为'2016-12-20'
    '''
    return date[0:4] + '-' + date[4:6] + '-' + date[6:8]


def DateFormat(date,
               date_format_after='%Y-%m-%d',
               date_format_before='%Y%m%d'):
    '''
    格式化日期:
    默认将日期转化为：2016-01-01 格式
    带有十分秒的日期格式：'%Y-%m-%d %H:%M:%S'
    20160101 -> 2016-01-01 或 2016-01-01 00:00:00
    2016-01-01 -> 20160101 或 2016-01-01 00:00:00
    2016-01-01 00:00:00 -> 2016-01-01 或 20160101
    以及 02/Oct/2017:03:11:02 格式的日期转换
    例：
    DateFormat(date)
    DateFormat(date,'%Y%m%d')
    DateFormat(date,'%Y-%m-%d %H:%M:%S')
    '''
    if '-' in date:
        if ':' in date:
            date_format_before = '%Y-%m-%d %H:%M:%S'
        else:
            date_format_before = '%Y-%m-%d'

    return datetime.datetime.strftime(
        datetime.datetime.strptime(date, date_format_before),
        date_format_after)


def get_rank(df, column, num=500, ascend=False):
    '''
    获取当前df的前n名数据，并返回其排名
    默认降序排序,取前500名
    '''
    df = df.sort_values(by=column, ascending=ascend)[:num]
    df['rank'] = range(1, (len(df) + 1))
    return df


def nvl_data(col1, col2, result=0):
    '''
    判断col2是否为0，如果为0，返回result，如果不为0，则返回相除后的结果
    '''
    if col2 == 0:
        return result
    else:
        return col1 * 1.0 / col2


def sql_to_df(sql, db=None):
    # 将mysql检索出结果放入dataframe中返回
    db = db or settings_dev.platform
    url = mysql_template.format(db=db)
    engine = sqlalchemy.create_engine(url)
    conn = engine.raw_connection()
    try:
        df = pd.read_sql(sql, conn)
    finally:
        conn.close()
    return df


def timestamp_datetime(value):
    '''
    linux时间戳转正常时间
    value为传入的值为时间戳(整形)，如：1332888820
    经过localtime转换后变成
    time.struct_time(tm_year=2012, tm_mon=3, tm_mday=28, tm_hour=6, tm_min=53, tm_sec=40, tm_wday=2, tm_yday=88, tm_isdst=0)
    最后再经过strftime函数转换为正常日期格式。
    '''
    format = '%Y-%m-%d %H:%M:%S'
    value = time.localtime(value)
    dt = time.strftime(format, value)
    return dt

def md5(str):
    '''
    md5 加密
    '''
    m = hashlib.md5()
    m.update(str)
    return m.hexdigest()


if __name__ == "__main__":
    # import doctest
    # doctest.testmod()
    # l = [['ds=20160421/paylog_20160422', 'raw_paylog', 'sanguo_ks'],
    #      ['ds=20160421/paylog_20160422', 'raw_paylog', 'sanguo_ks', '20160422']]
    # load_data_to_hive(l)
    # t = timestamp_to_string(0)
    # print t
    # tp = string_to_timestamp(t)
    # print tp
    # print date_range('20160301', '20160302')
    # print format_dates(['20160101'])
    # print format_dates(['20160101', '20160102'])
    settings_dev.set_env('sanguo_ks')
    print get_server_list()
