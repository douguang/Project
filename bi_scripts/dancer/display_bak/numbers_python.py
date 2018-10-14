#!/usr/bin/env python
# -*- coding: UTF-8 -*-
import os
for filename in os.listdir(r'/Users/kaiqigu/bi_scripts/dancer/display'):
    if filename.startswith('dis_'):
        print filename
        os.system("python %s" % filename)
