#!/usr/bin/env python
# -- coding: UTF-8 --
'''
@Time : 2017/7/26 0026 10:21
@Author : Zhang Yongchen
@File : demo.py.py
@Software: PyCharm Community Edition
Description :
'''
import pandas as pd
from sqlalchemy.engine import create_engine
import datetime
import struct
from socket import inet_aton
import os

_unpack_V = lambda b: struct.unpack("<L", b)
_unpack_N = lambda b: struct.unpack(">L", b)
_unpack_C = lambda b: struct.unpack("B", b)

class IP:
    offset = 0
    index = 0
    binary = ""

    @staticmethod
    def load(file):
        try:
            path = os.path.abspath(file)
            with open(path, "rb") as f:
                IP.binary = f.read()
                IP.offset, = _unpack_N(IP.binary[:4])
                IP.index = IP.binary[4:IP.offset]
        except Exception as ex:
            print "cannot open file %s" % file
            print ex.message
            exit(0)

    @staticmethod
    def find(ip):
        index = IP.index
        offset = IP.offset
        binary = IP.binary
        nip = inet_aton(ip)
        ipdot = ip.split('.')
        if int(ipdot[0]) < 0 or int(ipdot[0]) > 255 or len(ipdot) != 4:
            return "N/A"

        tmp_offset = int(ipdot[0]) * 4
        start, = _unpack_V(index[tmp_offset:tmp_offset + 4])

        index_offset = index_length = 0
        max_comp_len = offset - 1028
        start = start * 8 + 1024
        while start < max_comp_len:
            if index[start:start + 4] >= nip:
                index_offset, = _unpack_V(index[start + 4:start + 7] + chr(0).encode('utf-8'))
                index_length, = _unpack_C(index[start + 7])
                break
            start += 8

        if index_offset == 0:
            return "N/A"

        res_offset = offset + index_offset - 1024
        return binary[res_offset:res_offset + index_length].decode('utf-8')

ltv_days = [1, 2, 3, 4, 5, 6, 7, 14, 30, 60, 90, 120, 150, 180]

def hql_to_df(sql):
    impala_url = 'impala://192.168.1.47:21050/dancer_mul'
    engine = create_engine(impala_url)
    connection = engine.raw_connection()
    print '''===RUNNING==='''
    print sql
    df = pd.read_sql(sql, connection)
    # print df
    return df
    connection.close()

def ds_add(date, delta, date_format='%Y%m%d'):
    return datetime.datetime.strftime(
        datetime.datetime.strptime(date, date_format) +
        datetime.timedelta(delta), date_format)

def ds_delta(start, end, date_format='%Y%m%d'):
    now = datetime.datetime.strptime(start, date_format)
    end = datetime.datetime.strptime(end, date_format)
    if now <= end:
        delta = (end - now).days
        return delta

def date_range(start, end, date_format='%Y%m%d'):
    # result = []
    now = datetime.datetime.strptime(start, date_format)
    end = datetime.datetime.strptime(end, date_format)
    while now <= end:
        # result.append(now.strftime(date_format))
        yield now.strftime(date_format)
        now += datetime.timedelta(days=1)

def formatDate(Date, formatType='YYYY-MM-DD'):
    formatType = formatType.replace('YYYY', Date[0:4])
    formatType = formatType.replace('MM', Date[4:6])
    formatType = formatType.replace('DD', Date[-2:])
    return formatType


def dis_ip_country_ltv(date):
    # 根据要求的LTV天数倒推需要的开始日期
    start_date = ds_add(date, -max(ltv_days) + 1)  # 增加时间判断，11月25日起进行计算
    dates_to_update = date_range('20170629', date)
    f_start_date = formatDate(start_date)
    f_date = formatDate(date)
    # 注册数据
    reg_ip_sql = '''
    SELECT regexp_replace(to_date(regist_time),'-','') AS regist_time,
           account,
           regist_ip
    FROM
      ( SELECT regist_time,
               account,
               regist_ip,
               row_number() over(partition BY account
                                 ORDER BY regist_time) AS rn
       FROM mid_info_all
       WHERE ds = '{date}'
         AND to_date(regist_time) >= '2016-11-25'
         AND to_date(regist_time) <= '{f_date}') t1
    WHERE t1.rn=1
    '''.format(date=date, f_start_date=f_start_date, f_date=f_date)
    # print reg_ip_sql
    reg_ip_df = hql_to_df(reg_ip_sql)
    reg_ip_df.fillna(0)
    reg_ip_df['regist_ip'] = reg_ip_df['regist_ip'].astype(basestring)
    IP.load(os.path.abspath("./tinyipdata_utf8.dat"))

    def ip_lines():
        for _, row in reg_ip_df.iterrows():
            ip = row.regist_ip
            try:
                country = IP.find(ip).strip().encode("utf8")
                if '中国台湾' in country:
                    country = '台湾'
                elif '中国香港' in country:
                    country = '香港'
                elif '中国澳门' in country:
                    country = '澳门'
                elif '中国' in country:
                    country = '中国'
                yield [row.regist_time, row.account, country]
            except:
                pass
    reg_df = pd.DataFrame(ip_lines(), columns=['ds', 'account', 'country'])
    print reg_df.head(10)

    # 所有的充值数据
    pay_sql = '''
    SELECT ds,
           account,
           pay_rmb
    FROM
      ( SELECT ds,
               user_id,
               sum(order_money) AS pay_rmb
       FROM raw_paylog
       WHERE platform_2 != 'admin_test'
         AND ds >= '{start_date}'
         AND ds <= '{date}'
         AND order_id NOT LIKE '%test%'
       GROUP BY ds,
                user_id ) pay
    JOIN
      ( SELECT user_id,
               account
       FROM mid_info_all
       WHERE ds = '{date}' ) info ON info.user_id = pay.user_id
    '''.format(start_date=start_date, date=date)
    pay_df = hql_to_df(pay_sql)
    print pay_df.head(10)

    # LTV计算主体
    dfs = []
    for date_to_update in dates_to_update:
        days_list = []
        if date_to_update >= '20170629':
            use_reg_df = reg_df[(reg_df.ds == date_to_update)].copy()
            ltv_dfs = use_reg_df.groupby(['ds', 'country']).agg({'account': lambda g: g.nunique()}).reset_index().rename(
                columns={
                    'account': 'reg_user_num'
                })
            for ltv_ds in ltv_days:
                ltv_end_date = ds_add(date_to_update, ltv_ds - 1)
                if ltv_end_date <= date:
                    use_pay_df = pay_df[(pay_df.ds <= ltv_end_date) & (
                        pay_df.ds >= date_to_update)][['account', 'pay_rmb']]
                    use_pay_df['pay_num'] = use_pay_df['account']
                    result_ltv_df = use_reg_df.merge(
                        use_pay_df, on='account', how='left')
                    result_ltv_df = result_ltv_df.groupby(['ds', 'country']).agg({
                        'account': lambda g: g.nunique(),
                        'pay_num': lambda g: g.nunique(),
                        'pay_rmb': lambda g: g.sum()
                    }).reset_index().rename(columns={
                        'account': 'reg_user_num',
                        'pay_num': 'd%s_pay_num' % ltv_ds,
                        'pay_rmb': 'd%s_pay_rmb' % ltv_ds
                    }).fillna(0)
                    result_ltv_df['d%s_ltv' % ltv_ds] = result_ltv_df['d%s_pay_rmb' % ltv_ds] * 1.0 / result_ltv_df[
                        'reg_user_num']
                    ltv_dfs = ltv_dfs.merge(
                        result_ltv_df, on=['ds', 'country', 'reg_user_num'])
                else:
                    days = ds_delta(date_to_update, ltv_end_date) + 1
                    days_list.append(days)
                    for ltv_date in ltv_days:
                        if ltv_date in days_list:
                            ltv_dfs['d%s_pay_num' % ltv_date] = 0
                            ltv_dfs['d%s_pay_rmb' % ltv_date] = 0
                            ltv_dfs['d%s_ltv' % ltv_date] = 0
            columns = sum((['d%d_ltv' % ltv_day, ]
                           for ltv_day in ltv_days), ['ds', 'reg_user_num', 'country'])
            ltv_dfs = ltv_dfs[columns]
            dfs.append(ltv_dfs)
        else:
            continue
    final_result = pd.concat(dfs)
    return final_result

if __name__ == '__main__':

    date = raw_input("date:")
    result = dis_ip_country_ltv(date)
    f_name = raw_input("name is ?")
    result.to_excel('%s.xlsx' % f_name, index=True)