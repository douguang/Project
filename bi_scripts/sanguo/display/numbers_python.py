#!/usr/bin/env python
# -*- coding: UTF-8 -*-
import os
for filename in os.listdir(r'/Users/kaiqigu/bi_scripts/sanguo/display'):
    if filename.startswith('dis_'):
        print filename
for filename in ['dis_act_3day_card_evo.py', 'dis_act_3day_card_using_rate.py', 'dis_days_coin_spend.py']:
    print filename
    os.system("python %s" % filename)
