#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Dong Junshuang
Description : 充值月卡
'''
import settings_dev
from utils import hql_to_df, hqls_to_dfs
from pandas import DataFrame

if __name__ == '__main__':
    settings_dev.set_env('dancer_tw')

sql = '''
SELECT ds,count(distinct user_id) uid_num
    FROM mid_actionlog
    WHERE ds >= '20160907'
    and ds <= '20160917'
    and a_typ = 'pay_award.get_month_award'
    group by ds
    order by ds
'''
