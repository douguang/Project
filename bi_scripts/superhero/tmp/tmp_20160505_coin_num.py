#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Dong Junshuang
Description : 泰国4月1日-5月3日钻石存量数据
'''
import settings
from utils import hql_to_df
import pandas as pd

settings.set_env('superhero_tl')

sql = '''
    select  ds
            ,sum(zuanshi) as coin_num
    from    mid_info_all
    where   ds >= '20160401' and ds <= '20160503'
    group by ds
    order by ds
'''
