# -*- coding: UTF-8 -*-
# import gzip
# uid_list = []
# id_list = open('uid.txt')
# for i in id_list:
#     uid = i.strip()
#     uid_list.append(uid)
#
# for d in range(25, 26):
#     log_nginx = gzip.open('access.log-201704%s.gz' % d)
#     jg = open('result_%s.txt' % d, 'a+')
#     for n in log_nginx:
#         try:
#             if 'forward.forward' in n and 'result' in n:
#                 uid = n.split('user_token=')[1].split('&')[0].strip()
#                 if uid in uid_list:
#                     method = n.split('method=')[1].split('&')[0].strip()
#                     temp = int(n.split('__ts=')[1].split('&')[0].strip())
#                     result = n.split('result=')[1].split('&')[0].strip()
#                     jg.write('%s\t%s\t%s\t%s\n' % (uid, temp, method, result))
#         except Exception, e:
#             print e
#             print n
# log_nginx.close()
# jg.close()






# -*- coding: utf-8 -*-
import json

__version__ = '0.0.1'
__author__ = 'Elyes Du <lyxint@gmail.com>'
__url__ = 'https://github.com/lyxint/xxtea'

try:
    import binascii.b2a_hex as str2hex
    import binascii.a2b_hex as hex2str
except ImportError:

    def hex2str(s):
        length = len(s)
        result = ''
        for i in xrange(0, length, 2):
            result += chr(int(s[i:i+2], 16))
        return result

    def str2hex(s):
        table = "0123456789abcdef"
        length = len(s)
        result = ''
        for i in xrange(length):
            result += table[ord(s[i])>>4] + table[ord(s[i])&0xF]
        return result


def str2longs(s):
    length = (len(s) + 3) / 4
    s = s.ljust(length*4, '\0')
    result = []
    for i in xrange(length):
        j = 0
        j |= ord(s[i*4])
        j |= ord(s[i*4+1])<<8
        j |= ord(s[i*4+2])<<16
        j |= ord(s[i*4+3])<<24
        result.append(j)
    return result

def longs2str(s):
    result = ""
    for c in s:
        result += chr(c&0xFF) + chr(c>>8&0xFF)\
               + chr(c>>16&0xFF) + chr(c>>24&0xFF)
    return result.rstrip('\0')

def btea(v, n, k):
    if not isinstance(v, list) or \
        not isinstance(n, int) or \
        not isinstance(k, (list, tuple)):
        return False

    MX = lambda: ((z>>5)^(y<<2)) + ((y>>3)^(z<<4))^(sum^y) + (k[(p & 3)^e]^z)
    u32 = lambda x: x & 0xffffffff

    y = v[0]
    sum = 0
    DELTA = 0x9e3779b9
    if n > 1:
        z = v[n-1]
        q = 6 + 52 / n
        while q > 0:
            q -= 1
            sum = u32(sum + DELTA)
            e = u32(sum >> 2) & 3
            p = 0
            while p < n - 1:
                y = v[p+1]
                z = v[p] = u32(v[p] + MX())
                p += 1
            y = v[0]
            z = v[n-1] = u32(v[n-1] + MX())
        return True
    elif n < -1:
        n = -n
        q = 6 + 52 / n
        sum = u32(q * DELTA)
        while sum != 0:
            e = u32(sum >> 2) & 3
            p = n - 1
            while p > 0:
                z = v[p-1]
                y = v[p] = u32(v[p] - MX())
                p -= 1
            z = v[n-1]
            y = v[0] = u32(v[0] - MX())
            sum = u32(sum - DELTA)
        return True
    return False

def encrypt(str, key, returnhex=True):
    key = key.ljust(16, '\0')
    v = str2longs(str)
    k = str2longs(key)
    # print '11111', k, v
    n = len(v)
    btea(v, n, k)

    # print '11111encrypt', v
    #v = [2551591592, 1523315145, 24993238, 4080817971, 4273368954]
    result = longs2str(v)
    if (returnhex):
        result = str2hex(result)
    return result

def decrypt(s, key, ishex=True):
    key = key.ljust(16, '\0')
    if (ishex):
        s = hex2str(s)
    v = str2longs(s)
    k = str2longs(key)
    n = len(v)

    btea(v, -n, k)

    return longs2str(v)

def xxtea_encrypt(data, key=None):
    """用xxtea加密数据
    """
    key = 'Kqg-+9Myfront1*/'

    return encrypt(data, key, returnhex=True)


def xxtea_decrypt(data, key=None):
    """用xxtea解密数据
    """
    key = 'Kqg-+9Myfront1*/'

    return decrypt(data, key, ishex=True)


def decrypt_battle_result(data, key=None):
    """解密前端战斗数据，返回json数据
    """
    result = xxtea_decrypt(data, key)
    # # 处理前端的bug 去除字符串尾部的乱七八糟的数据
    # # eg: {"page":"0","is_win":1,"stage_step":"1","diffculty_step":"0","chapter":"3"}\x00K
    # suffix_pos = result.rfind('}')
    # real_result = result[:suffix_pos + 1]
    # return json.account(result)
    return result

if __name__ == '__main__':
    result = '1e83d5f4912a16671cfd5f338d594a554f7f4248ba34ad58b4da0dabc6d02e9167e8bdd169eb5e2a4f927e4a56127785f44725cae4f23037cb2fd772781dd2ae796b8a3f6b75fc0b1034ff04aa319b5b25cebe9eff4248987ee5227056e89191759a5f454501ee8539a0518604cc6e62ec89f919d2222fb1b40995dffd9af39f1b3e51c90154ee4b9a0aa58b8b2688caa97e03c87ca7c96310f3a692a60bdf07f35bd9713001f5978aa2024cddea9c925dbfe4869c9d225b37e5d0d79426931bcaf78d8b64b3b1ef5fe24b8db530d0c779befc39b906ae948e68f823f35205b90d9209c32b816ff0107e072add6bebfc8a99e2a365475afb598beb3c976285f554f507371b66ce85782afce771342ead56f6528e3fa565eb09228ccf75824b6f1b3485b4c7f09e3d215fb9bdf7cd7c53abbe6f85e851e628b830e90c8e713ab611c5603f0e5599fa41ae1a650fdda099b92b38394c7e20e0bde0d250c1e842d6d2793eef53b4b481900d1afacbc522b028e282ab25ae325baa2dc4adee4db0824ec4a49d6405a16e2711683917ddee589114ba8fb1578d11dad3fc61aef4b0241a0ecddd3a38a91a99f318e78a047956d8a62b4a989ea479646adcf7a2bc271c89d47cee7ddd7a3a4e6654a8b3f74c728534bc7be387c5390ee153c783dc2e2fb44f47767dbc6f8b084ac011f5da7e0f59d65a0f7a47221928b868ff9e25d65b645363326d2c746b83ca9f0b29c68bcbfa2406b86fd592a8e7c59a9d44fb442e9e8ec65d'
    print decrypt_battle_result(result, key=None)

    log = open(r'E:\Data\output\dancer\a.txt')
    jg = open(r'E:\Data\output\dancer\analysis.txt', 'a+')
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
                        jg.write('%s\t%s\t%s\t%s\t%s\n' % (mark, result, item_list[0], item_list[2], item_list[-1]))
        except Exception, e:
            print i
            print e
    print 'over'
    log.close()
    jg.close()
