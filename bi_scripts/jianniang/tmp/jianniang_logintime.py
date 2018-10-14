#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
>>> 通过动作日志计算在线时长
"""
import json
import time
import pandas as pd
from pandas import DataFrame

zd = {}


def analysis_json(line):
    l = json.loads(line)
    account = l.get('account', '')
    log_t = int(l.get('log_t', ''))
    if account != '':
        if account not in zd:
            zd[account] = [log_t, 0, 1]
        else:
            if log_t > zd[account][0]:
                login = log_t - zd[account][0]
                if login >= 300:
                    zd[account][2] += 1
                else:
                    zd[account][0] = log_t
                    zd[account][1] += login
            else:
                print log_t, zd[account][0]
                print 'error'


log = open(r'C:\Users\woodc\Downloads\temp.log')
for line in log:
    analysis_json(line)
df = pd.DataFrame(zd)
new_df = df.T
new_df.to_excel(r'C:\workflow\bi_scripts\jianniang\tmp\user_login.xlsx', index=False)
