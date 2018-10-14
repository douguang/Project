#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Dong Junshuang
Description :
'''
import calendar
import sqlalchemy
from hdfs import InsecureClient
import os
import settings_dev
import datetime
from settings_dev import hive_template, impala_template
import time


def ds_add(date, delta, date_format='%Y%m%d'):
    '''日期计算,delta: 间隔天数，可为负数
    ds_add('20160301', 2) -> '20160303'
    ds_add('20160301', -2) -> '20160228'
    ds_add('2016-03-01', -2, '%Y-%m-%d') -> '2016-02-28'
    '''
    return datetime.datetime.strftime(
        datetime.datetime.strptime(date, date_format) +
        datetime.timedelta(delta), date_format)


def DateFormat(date,
               date_format_after='%Y-%m-%d',
               date_format_before='%Y%m%d'):
    '''格式化日期:默认:20160101 -> 2016-01-01 格式
    '%Y-%m-%d %H:%M:%S'
    例：DateFormat(date)
    DateFormat(date,'%Y%m%d')
    DateFormat(date,'%Y-%m-%d %H:%M:%S')
    '''
    return datetime.datetime.strftime(
        datetime.datetime.strptime(date, date_format_before),
        date_format_after)


def ColorPrint(message, color=32):
    '''输出带有日期和颜色的信息，32表示绿色，31表示红色
    '''
    time_str = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
    message = time_str + '\t' + message
    print_message = "\033[1;{color};40m {message} \033[0m!".format(
        color=color, message=message)
    print print_message


def error_log(message, date=None):
    '''错误日志存储，date格式：2016-01-01
    '''
    date = date or ((datetime.datetime.now() + datetime.timedelta(days=-1)
                     ).strftime('%Y-%m-%d'))
    log_path = os.path.join(settings_dev.BASE_ROOT, 'error_log', date)
    # print log_path
    f = open(log_path, 'a+')
    message = ': WARNING :' + message
    print ColorPrint(message, color=31)
    f.write(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())) +
            message + '\n')
    f.close()


def run_hql(hql, db='', server='hive'):
    '''执行hql语句
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


def check_hive_data(hive_path, date):
    # 检查hive中文件是否存在, 并将错误信息存入日志
    hdfs_client = InsecureClient(settings_dev.hdfs_url)
    engine = sqlalchemy.create_engine(hive_template.format(db=''))
    conn_hive = engine.raw_connection()
    try:
        return hdfs_client.status(hive_path, strict=False)
    except Exception, e:
        print e
        error_log('检查hive异常', DateFormat(date))
        return 0
    finally:
        conn_hive.close()


def check_exist(platform, table, ds_str):
    hdfs_client = InsecureClient(settings_dev.hdfs_url)
    dir_hdfs_path = '/user/hive/warehouse/{platform}.db/{table}'.format(
        platform=platform, table=table)
    dir_hdfs_path_ds = '/user/hive/warehouse/{platform}.db/{table}/ds={ds_str}'.format(
        platform=platform, table=table,
        ds_str=ds_str)
    path_list = [path for path, dirs, files in hdfs_client.walk(dir_hdfs_path)]

    if dir_hdfs_path_ds in path_list:
        # print path
        # print len(files)
        files = [files for path, dirs,
                 files in hdfs_client.walk(dir_hdfs_path_ds)][0]
        if len(files) > 0:
            return True
        else:
            print '{dir_hdfs_path} Is Not File !'.format(
                dir_hdfs_path=dir_hdfs_path)
            return False
    else:
        print 'Not Dirs: {dir_hdfs_path_ds}'.format(
            dir_hdfs_path_ds=dir_hdfs_path_ds)
        return False


def get_remote(hdfs_path, local_path, date, table, db):
    try:
        hdfs_client = InsecureClient(settings_dev.hdfs_url)
        engine = sqlalchemy.create_engine(hive_template.format(db=''))
        conn_hive = engine.raw_connection()
        cur = conn_hive.cursor()
        remote_path = hdfs_client.upload(hdfs_path, local_path, overwrite=True)
        if remote_path:
            hql = '''
            load data inpath '{remote_path}'
            into table {db}.{table}
            partition (ds='{date}')
            '''.format(**{
                'date': date,
                'remote_path': remote_path,
                'table': table,
                'db': db,
            })
            cur.execute(hql)
            print 'Success: {local_path} -> {db}.{table} partition: {date}\n'.format(
                **{
                    'table': table,
                    'db': db,
                    'local_path': local_path,
                    'date': date
                })
        return remote_path
    except Exception, e:
        print e
    finally:
        cur.close()
        conn_hive.close()


def load_data_to_file(hive_path, local_path, db, table, date, filename=None):
    '''导入数据至hive
    local_path:/home/data/qiling_ks/redis_stats/info_20170101
    hive_path:/user/hive/warehouse/qiling_ks_bak.db/raw_info/ds=20170101/info_20170101
    '''
    filename = filename or os.path.basename(local_path)
    hdfs_path = '/tmp/{db}/{filename}'.format(**{
        'filename': filename,
        'db': db
    })
    if check_hive_data(hive_path, date):
        print '{local_path} already in hive!'.format(local_path=local_path)
    else:
        if os.path.exists(local_path):
            i = 0
            while i < 20:
                ret_remote_path = get_remote(hdfs_path, local_path, date,
                                             table, db)
                if ret_remote_path:
                    i = 21
                else:
                    i += 1
                    time.sleep(3)
            if i == 20:
                error_log('{local_path} 20 times not upload to hdfs'.format(
                    local_path=local_path),
                    DateFormat(date))
                raise NameError(
                    '{hdfs_path} 20 times not upload to hdfs'.format(
                        hdfs_path=hdfs_path),
                    DateFormat(date))
        else:
            error_log('{local_path} not in local'.format(
                local_path=local_path),
                DateFormat(date))
            raise NameError('{local_path} not in local'.format(
                local_path=local_path))


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


def check_week_month(date, mw, num=3):
    # 用于判断是否是星期几(默认周三) 和 是否是月末
    # mw用户判断是根据月还是周判断'm','w'
    if mw.upper() == 'M':
        if date[6:8] == str(calendar.monthrange(int(date[:4]), int(date[4:6]))[1]):
            return True
    if mw.upper() == 'W':
        if num - 1 == datetime.datetime.strptime(date, '%Y%m%d').weekday():
            return True


def Parse_data(f_in, f_out, parse_prossing):
    '''f_in:输入的文件（路径加文件名）,f_out:输出的文件（路径加文件名）
    parse_prossing:解析文件的具体操作
    '''
    with open(f_in) as f_in:
        with open(f_out, 'w') as f_out:
            for line in f_in:
                try:
                    line_out = parse_prossing(line)
                    f_out.write(line_out)
                except KeyboardInterrupt:
                    raise
                except:
                    pass
                    # 解析错误
                    # print line

def Parse_data_dict(f_in, f_out, parse_prossing):
    '''
    将卡牌大字典解析成单条卡牌数据
    f_in:输入的文件（路径加文件名）,f_out:输出的文件（路径加文件名）
    parse_prossing:解析文件的具体操作
    '''
    with open(f_in) as f_in:
        with open(f_out, 'w') as f_out:
            for line in f_in:
                try:
                    line_out = parse_prossing(line)
                    for i in line_out:
                        f_out.write(i)
                except KeyboardInterrupt:
                    raise
                except:
                    print line

if __name__ == '__main__':
    db = 'qiling_ks_bak'
    partition = '20161230'
    settings_dev.set_env(db)
    # check_week_month(date, 'y')
    # # 导入所有表
    # load_data_to_file(db, partition)
    # # 导入指定表
    # load_data_to_file(db, partition, ['raw_info','raw_paylog'])
