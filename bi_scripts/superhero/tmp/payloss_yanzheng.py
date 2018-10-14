select count(account)  from (
SELECT account,to_date(min(create_time)) reg,max(uid) user_id
FROM mid_info_all
where substr(uid,1,1) = 'a' and ds = '20160611' and create_time != '1970-01-01 08:00:00' and fresh_time != '1970-01-01 08:00:00'
group by account
having to_date(min(create_time)) = to_date('2016-06-07')
)  a where reg = to_date('2016-06-07')
and user_id in (select user_id from mid_paylog_all where substr(user_id,1,1) = 'a' and ds = '20160611')
