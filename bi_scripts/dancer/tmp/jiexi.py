from jiexi_tools import *
log = open('/home/kaiqigu/Documents/result_1.txt')
jg = open('/home/kaiqigu/Documents/analysis.txt', 'a+')
for i in log:
	try:
		mark = i.split('\t')[0].strip()
		print mark
		result = i.split('\t')[1].strip()
		print result
		analysis = str(decrypt_battle_result(result))
		print analysis
		print '21231'
		for card_id, card_info in eval(analysis).iteritems():
			if '-' in str(card_info):
				for item in card_info:
					item_list = list(item)
					jg.write('%s\t%s\t%s\t%s\t%s\n' %  (mark, result, item_list[0], item_list[2], item_list[-1]))
	except Exception,e:
		print i
		print e
print 'over'
log.close()
jg.close()