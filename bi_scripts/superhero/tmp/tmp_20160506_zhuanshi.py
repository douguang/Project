#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Hu Wenchao
Description : 卡牌转世分析
'''
import pandas as pd

pt_path = {
    # 'qiku': '/home/data/superhero_qiku/redis_stats',
    'pub': '/home/data/superhero/log_redis'
}

reborn_after_before = {
    32300: 6500,
    32400: 8600,
    32500: 9100,
    32600: 6400,
    60500: 6800,
    60600: 6900,
    60700: 7000,
    60800: 7700,
}
card_cids = reborn_after_before.keys() + reborn_after_before.values()
for pt, path in pt_path.iteritems():
    cards_to_analysis_dfs = []
    for d in range(1, 21):
        date = '201604%02d' % d
        df = pd.read_csv('%s/card_%s' % (path, date), header=None, index_col=False, sep='\t')
        cards_to_analysis_dfs.append(df[df[4].isin(card_cids)])
    cards_to_analysis_df = pd.concat(cards_to_analysis_dfs)


