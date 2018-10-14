#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
Author      : Dong Junshuang
Description : 超级英雄 pubV6以上3天活跃玩家高级能晶存量
create_date : 2016.05.05
comment     : execute this sql in impala
'''

sql = '''
    select  uid
            ,vip_level
            ,nengjing
            ,gaojinengjing
    from
    (
        select  uid
                ,nengjing
                ,gaojinengjing
                ,vip_level
        from    mid_info_all
        where   ds = '20160504'
    ) t1
    left semi join
    (
        select  distinct uid
        from    raw_info
        where   ds >= '20160502'
        and     ds <= '20160504'
    ) t2 on t1.uid = t2.uid
    where  t1.vip_level >=6
    order by vip_level
    '''
