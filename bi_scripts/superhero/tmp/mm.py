INVALIDATE METADATA;
select ds,count(distinct uid) as uid_num from raw_action_log where action='god_field.god_field_battle'
and ds >= '20160427' and ds<='20160512'
group by ds


load data inpath '/home/data/superhero_vietnam/redis_stats/info_20160427'
        into table superhero_vt.raw_info
        partition (ds='20160427')
