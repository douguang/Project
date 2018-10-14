#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Hu Wenchao
Description : pub和七酷每天的钻石异常
'''
import os
from utils import timestamp_to_string
import sys
import datetime

try:
    d = sys.argv[1]
except:
    d = (datetime.date.today() - datetime.timedelta(days=1)).strftime('%Y%m%d')

paths = {
    'pub': '/home/data/superhero_qiku/log_temp/qq_action_log',
    'qiku': '/home/data/superhero/action_log/action_log'
}

last_uid_action_coin = {}   # uid: [tp, action, post_coin, pre_coin]
for pt, path in paths.iteritems():
    if not os.path.exists('%s_%s_sorted' % (path, d)):
        command = 'sort %s_%s > %s_%s_sorted' % (path, d, path, d)
        print command, 'start!'
        os.system(command)
        print command, 'done!'
    with open('%s_%s_sorted' % (path, d)) as f, open('/home/data/http_path/uids_yichang_coin_%s_%s.csv' % (pt, d), 'w') as f_out:
        for line in f:
            try:
                l = line.strip().split('\t')
                tp = float(l[0])
                uid = l[3]
                pre_coin = l[8]
                post_coin = float(l[26])
                if post_coin < 0:
                    continue
                action = l[40]
                if uid in last_uid_action_coin and post_coin - last_uid_action_coin[uid][2] > 40000:
                    latest = last_uid_action_coin[uid]
                    print timestamp_to_string(tp), uid, post_coin, latest[2]
                    f_out.write('%s,%s,%s,%s,%s\n' % (timestamp_to_string(latest[0]), uid, latest[1], latest[2], latest[3]))
                    f_out.write('%s,%s,%s,%s,%s\n' % (timestamp_to_string(tp), uid, action, post_coin, pre_coin))
                last_uid_action_coin[uid] = (tp, action, post_coin, pre_coin)
            except:
                # raise
                # f_error.write(line)
                pass
