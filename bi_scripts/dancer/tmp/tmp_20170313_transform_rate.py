#!/usr/bin/env python
# -*- coding: UTF-8 -*-
'''
@Time    : 17-1-23 上午11:14
@Author  : Andy
@File    : tmp_nginx_demo.py
@Software: PyCharm
Description :
'''
#!/usr/bin/env python
# -*- coding: UTF-8 -*-


import gzip
import os

guide_dict = {}
for filename in os.listdir(r'/home/data/sanguo_tl/nginx_log'):
    if filename in ('access.log-20170103.gz',
                    'access.log-20170104.gz',
                    'access.log-20170105.gz',
                    'access.log-20170106.gz',
                    'access.log-20170107.gz',
                    'access.log-20170108.gz',
                    'access.log-20170109.gz',
                    'access.log-20170110.gz',
                    'access.log-20170111.gz',
                    'access.log-20170112.gz',
                    'access.log-20170113.gz',
                    'access.log-20170114.gz',
                    'access.log-20170115.gz',
                    'access.log-20170116.gz',
                    'access.log-20170117.gz',
                    'access.log-20170118.gz',
                    'access.log-20170119.gz',
                    'access.log-20170120.gz',
                    'access.log-20170121.gz',
                    'access.log-20170122.gz',
                    'access.log-20170123.gz'):
        print filename
        file_log = gzip.open(filename)
        print "=="
        try:
            name = filename.split('-')[1].split('.')[0]
            final_dict={}
            #ss = open('user_ip_%s.txt' % name, 'w')
            for i in file_log:
                dic_dic={}
                if 'user_token' in i:
                    #print account
                    uid = i.split('user_token=')[1].split('&')[0].strip()
                    #print uid
                    if uid != '':
                        ip = i.split(' - -')[0].strip()
                        #print ip
                        ts = i.split('[')[1].split(']')[0].strip()
                        #print ts
                        ts_time = ts[:11]
                        m_list = ts_time.split('/')
                        if m_list[1] == 'Jan':
                            m_list[1] = '01'
                        ts_time = m_list[2] +'-' +m_list[1] +'-'+ m_list[0]
                        #print ts_time
                        if 'channel_id' in i:
                            channel_id = i.split('&channel_id=')[1].split('&')[0].strip()
                            if len(channel_id) == 0:
                                channel_id = 'channel_id'
                        else:
                            channel_id = 'channel_id'
                        #print channel_id
                        if 'server_name' in i :
                            server_name = i.split('&server_name=')[1]
                            server_name = server_name[:3]
                        else:
                            server_name = 'server_name'

                        if 'account' in i:
                            account = i.split('&account=')[1].split('&')[0].strip()
                            if len(account) == 0:
                                account = 'account'
                        else:
                            account = 'account'

                        #print account
                        key = uid
                        if key in guide_dict:
                            new_ls = guide_dict[key]
                            if ts_time < new_ls[0]:
                                #print new_ls[0]
                                new_ls[0]=ts_time
                                new_ls[1]=ip
                                new_ls[2]=ts
                                if channel_id != '' and channel_id != 'channel_id':
                                    new_ls[3]=channel_id
                                if server_name != '' and server_name != 'server_name':
                                    new_ls[4]=server_name
                                if account != '' and account != 'account':
                                    new_ls[5]=account
                                guide_dict[key]=new_ls
                                #print "*"
                            else:
                                if channel_id != '' and channel_id != 'channel_id':
                                    new_ls[3]=channel_id
                                if server_name != '' and server_name != 'server_name':
                                    new_ls[4]=server_name
                                if account != '' and account != 'account':
                                    new_ls[5]=account
                                guide_dict[key]=new_ls
                                #print '+'
                        else:
                            new_list=[]
                            # print 'uid:',uid
                            # print "account:",account
                            # print "ts_time:",ts_time
                            # print "ip:",ip
                            # print "ts:",ts
                            # print "channel_id:",channel_id
                            #print "server_name:",server_name
                            new_list.append(ts_time)
                            new_list.append(ip)
                            new_list.append(ts)
                            new_list.append(channel_id)
                            new_list.append(server_name)
                            new_list.append(account)
                            guide_dict[key] = new_list
                            #print '_'
                        #print "--"
        except Exception, e:
            print e
        # print "00"
        # print final_dict
        # for d in final_dict:
        #     ss.write('%s\t%s\t%s\t%s\t%s\t%s' % (d, guide_dict[d][0], guide_dict[d][1], guide_dict[d][2], guide_dict[d][3], guide_dict[d][4]))

print guide_dict
res = open('anew_ip.txt','w')
for d in guide_dict:
    res.write('%s\t%s\t%s\t%s\t%s\t%s\t%s\n' % (d,guide_dict[d][0],guide_dict[d][1],guide_dict[d][2],guide_dict[d][3],guide_dict[d][4],guide_dict[d][5]))
print 'over'
res.close()
exit()
