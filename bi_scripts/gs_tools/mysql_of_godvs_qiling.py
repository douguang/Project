#!/usr/bin/env python
# -*- coding: UTF-8 -*-
"""
Author      : Hu Chunlong
Description : 连接到godvs数据库
create_date : 2016.12.09
"""
from sqlalchemy.engine import create_engine
import pandas as pd
from utils import hql_to_df
from utils import update_mysql
import settings_dev
import datetime


def get_info(date):
    table = 'qiling_ks_vipuserinfo'
    # 获取前一天数据
    backtime = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
    print backtime
    try:
        godvs_url = "mysql+pymysql://root:60aa954499f7ab@192.168.1.27/godvs"
        engine = create_engine(godvs_url)
        connection = engine.raw_connection()
        get_sql = "select * from {0}".format(table)
        old_df = pd.read_sql(get_sql, connection)
        old_df['ds'] = date
        print 'get old infomation'
        try:
            # 获取需求数据
            use_old_df = old_df[['role_id', 'vip_username', 'vip_birthday',
                                 'vip_qq', 'vip_telephone', 'vip_wechat',
                                 'vip_last_time_conn']]
            print 'get_use_old_df'
            # 获取info数据
            dfs = []
            for platform in ['qiling_ks', 'qiling_tx', 'qiling_ios']:
                settings_dev.set_env(platform)
                sql = '''
                SELECT user_id as role_id,
                       name as role_name,
                       level,
                       vip as vip_level,
                       to_date(act_time) AS act_time
                FROM mid_info_all
                WHERE ds = '{date}'
                  AND vip >= 6
                  AND level > 10
                  AND user_id NOT IN
                   ( SELECT user_id
                    FROM raw_paylog
                    WHERE platform_2 = 'admin_test' )
                '''.format(date=date)
                df = hql_to_df(sql)
                # 如果是空表，则等待半小时后再执行，如果时间超过18点，则终止程序
                if df.empty:
                    print 'df is empty, wait for 30min'
                    exit()
                else:
                    dfs.append(df)
            print 'get_new_info'
            # 合并数据
            new_df = pd.concat(dfs)
            result_df = new_df.merge(use_old_df, on='role_id', how='left')
            columns = ['vip_username', 'vip_birthday', 'vip_qq',
                       'vip_telephone', 'vip_wechat', 'vip_last_time_conn',
                       'role_id', 'role_name', 'level', 'vip_level',
                       'act_time']
            result_df = result_df[columns].fillna('')
            print 'result_df'
        except Exception, e:
            print e
            print 'get_qiling_info_error'
            print 'run failed'
            exit()
        try:
            # 备份前一天数据到CSV格式
            old_df.to_csv(
                r'/home/data/bi_scripts/gs_tools/backup_data/backup_qiling_vipinfo_%s'
                % backtime,
                index=False)
            print 'qiling_vip_info_csv_backup'
            # 备份前一天数据到数据库
            back_table = 'backup_vip_info_qiling'
            back_del_sql = 'delete from {0} where ds="{1}"'.format(back_table,
                                                                   date)
            update_mysql(back_table, old_df, back_del_sql, 'godvs')
            print 'qiling_vip_info_mysql_backup'
        except Exception, e:
            print e
            print 'qiling_vip_info_backup_error'
            exit()
        # 删除数据
        cur = connection.cursor()
        del_sql = "delete from {0}".format(table)
        try:
            cur.execute(del_sql)
            connection.commit()
            print 'del data success'
            # 更新数据
            try:
                update_mysql(table, result_df, del_sql, 'godvs')
                print 'update new data success'
            except Exception, e:
                print e
                print 'update new data failed'
        except Exception, e:
            print e
            print 'delete data failed'
            connection.rollback()
    except Exception, e:
        print e
        print 'connection godvs_url failed'
        print 'run failed'
        exit()
    finally:
        connection.close()


if __name__ == '__main__':
    date = (
        datetime.date.today() - datetime.timedelta(days=1)).strftime('%Y%m%d')
    get_info(date)
