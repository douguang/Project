#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Hu Wenchao
Description : 4月活跃玩家钻石存量
'''

import pandas as pd
uids = pd.concat([pd.read_csv('/home/data/superhero_english/active_user/act_201604%02d' % i, header=None, index_col=None, sep='\t') for i in range(1, 28)])[0].unique()
info_df = pd.read_csv('/home/data/superhero_english/redis_stats/total_info_20160427', header=None, index_col=None, sep='\t')
uid_remain_coin = info_df[info_df[0].isin(uids)][[0, 14]]
uid_remain_coin.columns = [u'uid', u'钻石存量']
uid_remain_coin.to_excel('/tmp/superhero_en_remain_coin_20160428.xlsx')

