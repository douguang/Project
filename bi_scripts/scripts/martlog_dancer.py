#!/usr/bin/env python
# -*- coding: UTF-8 -*-

import datetime
import sys
import commands
import pandas as pd
import settings_dev
import os

for db in os.listdir(r'/home/data/bi_scripts/settings_dev'):
    if 'init' not in db:
        db = str(db).replace('.py', '')
        if 'dancer' in db:
            settings_dev.set_env(db)
            date_start = settings_dev.start_date
            date_end = '2017-02-23'
            for i in pd.date_range(str(date_start), str(date_end)):
                date = str(i).split(' ')[0].strip()
                print db, 'start_date:', date_start, 'now_date', date
                cmd = "cd /home/data/bi_scripts && /usr/local/bin/python -m luigi --workers 3 --module dancer.luigi_test.uniform_entry DancerDailyMart --date %s --platform %s" % (
                    date, db)
                commands.getoutput(cmd)
print 'over'
exit()
