#!/usr/bin/env python
# -*- coding: UTF-8 -*-
"""
Author      : Hu Chunlong
Description : 连接到godvs数据库
create_date : 2016.12.09
"""
from sqlalchemy.engine import create_engine
import pandas as pd
from utils import hql_to_df, update_mysql, ds_add
import settings_dev
import datetime


def get_info(date):
    table = 'country_ks_vipuserinfo'
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
        # 备份前一天数据到CSV格式
        try:
            old_df.to_csv(
                r'/home/data/bi_scripts/gs_tools/backup_data/backup_country_vipinfo_%s'
                % backtime,
                index=False)
            print 'country_vip_info_csv_backup'
        except Exception, e:
            print e
            print 'country_vip_info_csv_backup_error'
        # 备份前一天数据到数据库
        try:
            back_table = 'backup_vip_info_country'
            back_del_sql = 'delete from {0} where ds="{1}"'.format(back_table,
                                                                   date)
            update_mysql(back_table, old_df, back_del_sql, 'godvs')
            print 'country_vip_info_mysql_backup'
        except Exception, e:
            print e
            print 'country_vip_info_mysql_backup_error'
            print 'run failed'
            exit()
        try:
            # 获取需求数据
            use_old_df = old_df[['role_id', 'vip_username', 'vip_birthday',
                                 'vip_qq', 'vip_telephone', 'vip_wechat',
                                 'vip_last_time_conn']]
            print 'get_use_old_df'
            # 获取info数据
            dfs = []
            for platform in ['sanguo_ks']:
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
                if df.empty:
                    print 'df is empty, exit'
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
            print 'get_country_info_error'
            print 'run failed'
            exit()
    except Exception, e:
        print e
        print 'get_%s_old_data_failed' % ds_add(date, -1)
        print 'run failed'
        exit()
    finally:
        connection.close()


if __name__ == '__main__':
    date = (
        datetime.date.today() - datetime.timedelta(days=1)).strftime('%Y%m%d')
    get_info(date)
