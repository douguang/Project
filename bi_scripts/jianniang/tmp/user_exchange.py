#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
>>> 设备&用户转化率，使用actionlog的uuid统计
"""
# import json
import pandas as pd
from pandas import DataFrame
import settings_dev
from utils import hql_to_df
settings_dev.set_env('jianniang_test')


def user_exchange(date):
    sql = '''
SELECT a_typ,
        count(distinct account) AS account_num,
        count(uuid)
FROM parse_actionlog
WHERE ds = '{0}'
    AND to_date(log_t) = '2017-03-16'
GROUP BY a_typ
    '''.format(date)
    df = hql_to_df(sql)
    print df.head()

if __name__ == '__main__':
    date = '20170316'
    user_exchange(date)


# D = {}


# def analysis_json(line):
#     l = json.loads(line)
#     a_tar = l['body']['a_tar']
#     a_typ = l['body'].get('a_typ', '')
#     for ele in l['body'].get('a_tar', []):
#         if ele == 'uuid':
#             uuid = a_tar[ele]
#             print uuid
#             if uuid not in D:
#                 D[uuid] = a_typ


# log = open(r'C:\Users\woodc\Downloads\action_log_20170316', 'r')
# for line in log:
#     analysis_json(line)
# print D
