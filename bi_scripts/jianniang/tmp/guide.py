#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Dong Junshuang
Desctiption : 新手引导步骤停留
Time        : 2017-03-16
'''
import settings_dev
from utils import hql_to_df

if __name__ == '__main__':
    settings_dev.set_env('jianniang_test')
    # date = '20170316'
    # 活跃用户
    guide_sql = '''
    SELECT ds,
           user_id,
           guide_done
    FROM raw_guide
    WHERE ds >='20170524'
    AND user_id != 'None'
    '''
    # .format(date=date)
    reg_sql = '''
    SELECT DISTINCT regexp_replace(substr(reg_time,1,10),'-','') AS regtime,
                    user_id, platform, ds
    FROM raw_info
    WHERE ds >= '20170524'
      AND regexp_replace(substr(reg_time,1,10),'-','') >= '20170524'
    '''
    guide_df = hql_to_df(guide_sql)
    reg_df = hql_to_df(reg_sql)
    guide_df = guide_df.merge(reg_df, on=['ds', 'user_id'])
    guide_df['step'] = guide_df['guide_done'].map(
        lambda s: eval(s)[-1] if len(eval(s)) > 0 else 0)
    guide_df.to_excel(r'E:\Data\output\H5\guide_detail.xlsx')
    # step为0表示未进行新手引导
    # result_df = (guide_df.groupby('step').user_id.count().reset_index()
    #              .rename(columns={'user_id': 'uid_num'}))
    result_df = (guide_df.groupby(['ds', 'step']).user_id.count().reset_index()
                 .rename(columns={'user_id': 'uid_num'}))

    result_df.to_excel(r'E:\Data\output\H5\guide.xlsx')
