
def
sql = '''
SELECT ds,
       account,
       uid
FROM
  (SELECT ds,
          account,
          uid
   FROM raw_info
   WHERE ds= '{date}')a LEFT semi
JOIN
  (SELECT uid
   FROM raw_reg
   WHERE ds= '{date}')b ON a.uid=b.uid
WHERE account NOT IN
    (SELECT account
     FROM mid_info_all
     WHERE ds= '{date_ago}')
'''








