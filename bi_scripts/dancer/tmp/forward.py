# -*- coding: UTF-8 -*-
import gzip
uid_list = []
id_list = open('uid.txt')
for i in id_list:
    uid = i.strip()
    uid_list.append(uid)

for d in range(22, 23):
    log_nginx = gzip.open('access.log-201611%s.gz' % d)
    jg = open('result_%s.txt' % d, 'a+')
    for n in log_nginx:
        try:
            if 'forward.forward' in n and 'result' in n:
                uid = n.split('user_token=')[1].split('&')[0].strip()
                if uid in uid_list:
                    method = n.split('method=')[1].split('&')[0].strip()
                    temp = int(n.split('__ts=')[1].split('&')[0].strip())
                    result = n.split('result=')[1].split('&')[0].strip()
                    jg.write('%s\t%s\t%s\t%s\n' % (uid, temp, method, result))
        except Exception, e:
            print e
            print n
log_nginx.close()
jg.close()