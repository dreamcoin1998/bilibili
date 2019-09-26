import requests
import time
import json
from mysql import connector
import random
import xml.etree.ElementTree as ET
import multiprocessing as mp


user_agent = random.choice([
            "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/22.0.1207.1 Safari/537.1"
            "Mozilla/5.0 (X11; CrOS i686 2268.111.0) AppleWebKit/536.11 (KHTML, like Gecko) Chrome/20.0.1132.57 Safari/536.11",
            "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/536.6 (KHTML, like Gecko) Chrome/20.0.1092.0 Safari/536.6",
            "Mozilla/5.0 (Windows NT 6.2) AppleWebKit/536.6 (KHTML, like Gecko) Chrome/20.0.1090.0 Safari/536.6",
            "Mozilla/5.0 (Windows NT 6.2; WOW64) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/19.77.34.5 Safari/537.1"
            ])


'''UP主信息 所需API集合'''
# 用户信息 arg: mid    res: nickname, face_url, sex
user_info = 'https://api.bilibili.com/x/space/acc/info?mid='
# 用户粉丝数 arg: vmid(mid) res: star
user_star = 'https://api.bilibili.com/x/relation/stat?vmid='
# 用户视频播放数 arg: mi   res: upstat
user_upstat = 'https://api.bilibili.com/x/space/upstat?mid='
# 视频列表 获取UP主分区
video = 'https://space.bilibili.com/ajax/member/getSubmitVideos?mid=&page=1&pagesize=100'
'''视频信息 所需API集合'''
# 视频信息
video_info = 'https://api.bilibili.com/x/web-interface/view?aid='
# 视频标签
video_tags = 'http://api.bilibili.com/x/tag/archive/tags?aid='
# 弹幕 arg: cid
danmu = 'https://api.bilibili.com/x/v1/dm/list.so?oid='
# 获取cid arg: aid(av号)
get_cid = 'https://www.bilibili.com/widget/getPageList?aid='


# 增加重连次数
requests.adapters.DEFAULT_RETRIES = 5


# 获取视频列表
def VedioList(mid: str):
    # 获取数据并json解析
    res_video_list = requests.get('https://space.bilibili.com/ajax/member/getSubmitVideos?mid=%s&page=1&pagesize=100' % mid, headers={'User-Agent': user_agent}, timeout=30)
    up_video_list = json.loads(res_video_list.content)
    # UP主总视频数
    count = up_video_list['data']['count']
    # UP主所有视频的av号列表 所得为int类型的列表
    aid_list = []
    # 把第一页的aid添加到列表
    for d in up_video_list['data']['vlist']:
        aid_list.append(d['aid'])
    # 如果UP主总视频数量大于100， 把其余页aid添加到列表
    if count > 100:
        aid_list = aid_list + getVideoList(mid, count)
    # 获取所有的视频信息
    getVideoInfo(aid_list)
    return None


# 获取视频aid列表
def getVideoList(mid, count):
    # 总页数
    pages = count // 100 + 2
    aid_list = []
    for i in range(2, pages):
        res_video_list = requests.get('https://space.bilibili.com/ajax/member/getSubmitVideos?mid=%s&page=%s&pagesize=100' % (mid, str(i)), headers={'User-Agent': user_agent}, timeout=30)
        up_video_list = json.loads(res_video_list.content)
        for d in up_video_list['data']['vlist']:
            aid_list.append(d['aid'])
    return aid_list


# 获取视频信息
def getVideoInfo(aid_list: list):
    try:
        for i in aid_list:
            print('**********************')
            print('av号:', i)
            print('**********************')
            res_info = requests.get(video_info + str(i), headers={'User-Agent': user_agent}, timeout=30)
            res_tags = requests.get(video_tags + str(i), headers={'User-Agent': user_agent}, timeout=30)
            info = json.loads(res_info.content)
            sol_tags = json.loads(res_tags.content)
            # 数据
            # 标题
            title = info['data']['title']
            # 播放量
            upstat = info['data']['stat']['view']
            # 点赞量
            like = info['data']['stat']['like']
            # 投币量
            coins = info['data']['stat']['coin']
            # 收藏量
            favorite = info['data']['stat']['favorite']
            # 分享量
            share = info['data']['stat']['share']
            # 评论数
            reply = info['data']['stat']['reply']
            # 弹幕数
            danmau_num = info['data']['stat']['danmaku']
            # 发布时间 时间戳
            ctime = info['data']['ctime']
            if int(ctime) < 1546272001:
                print('时间不合适')
                continue
            # 作者mid
            mid = info['data']['owner']['mid']
            # 标签
            tags = sol_tags['data']
            tag_list = []
            for tag in tags:
                tag_list.append(tag['tag_name'])
            tag_list = json.dumps(tag_list)    # 标签列表 json
            print(tag_list)
            # 获取cid列表
            cid_list = getCid(str(i))
            print('cid:', cid_list)
            # 弹幕列表
            danmu_list = []
            # 根据cid获取弹幕XML文件,并解析然后添加到弹幕列表
            for j in cid_list:
                res_danmu = requests.get(danmu + j, headers={'User-Agent': user_agent}, timeout=30)
                dm = parseXml(res_danmu.content, j)
                danmu_list = danmu_list + dm
            # 弹幕列表 json类型
            # print(str(danmu_list))
            dm = json.dumps(danmu_list).replace("'", '')
            # danmu_json = json.dumps(danmu_list)
            # print(danmu_list)
            # danmu_data = '无'
            # 获取评论列表
            com_list = getAllCommentList(i)
            if com_list is not None:
                com_list_json = json.dumps(com_list).replace("'", '')    # 评论列表 json类型
            else:
                com_list_json = json.dumps('无').replace("'", '')
            # com_list_json = '无'
            print('res_info:', res_info.status_code)
            print('res_tags', res_tags.status_code)
            print('调用成功')
            # print(title, upstat, like, coins, favorite, share, reply, danmau_num, ctime, mid, tag_list, dm, com_list_json)
            # data.append([title, upstat, like, coins, favorite, share, reply, danmau_num, ctime, mid, tag_list, danmu_json, com_list_json])
            try:
                # 连接数据库
                db = connector.connect(host='localhost', user='gaojunbin', passwd='18759799353', database='bilibili')
                # 获取游标
                yb = db.cursor()
                yb.execute("INSERT INTO video(title, upstat, likes, coins, favorite, share, reply, ctime, tags, uid, danmu_num, danmu, comment) VALUES('%s', %d, %d, %d, %d, %d, %d, %d, '%s', %d, %d, '%s', '%s');" % (title, int(upstat), int(like), int(coins), int(favorite), int(share), int(reply), ctime, tag_list, int(mid), int(danmau_num), dm, com_list_json))
                # 提交执行
                db.commit()
                print('写入视频信息成功')
            except Exception as e:
                print(e)
                print('写入视频信息失败')
            # 关闭数据库
            finally:
                db.close()
                yb.close()
    except Exception as e:
        print(e)
        return None


# 获取视频cid
def getCid(aid: str):
    res_getcid = requests.get(get_cid + aid, headers={'User-Agent': user_agent}, timeout=30)
    # print(res_getcid.content)
    sol_getcid = json.loads(res_getcid.content)
    cid_list = []
    for i in sol_getcid:
        cid_list.append(str(i['cid']))
    print('成功获取cid')
    return cid_list


# 解析弹幕XML文件
def parseXml(xml_file, cid):
    with open(cid + '.xml', 'wb') as f:
        f.write(xml_file)
    danmu = []
    root = ET.parse(cid + '.xml')
    root = root.getroot()
    for child in root:
        if child.tag == 'd':
            # print(child.text)
            danmu.append(child.text)
    print("成功解析xml")
    # print(danmu)
    return danmu


def get_reply(num, step, q, aid):
    comment_list = []
    for n in range(num+1, num + 1 + step):
        time.sleep(random.randint(3, 4) / 10.1)  # 延迟时间，避免太快 ip 被封
        print('评论第%d页' % n)
        url = "https://api.bilibili.com/x/v2/reply?jsonp=jsonp&pn="+str(n)+"&type=1&oid="+str(aid)
        req = requests.get(url, headers={'User-Agent': user_agent}, timeout=30)
        text = req.text
        # print(text)
        with open(str(num) + '.html', 'w')as f:
            f.write(text)
        json_text_list = json.loads(text)
        if json_text_list["data"]["replies"] is not None:
            for i in json_text_list["data"]["replies"]:
                comment_list.append(i["content"]["message"])
        # print(comment_list)
    q.put(comment_list)


# 获取评论列表
def getAllCommentList(aid):
    url = "http://api.bilibili.com/x/reply?type=1&oid=" + str(aid) + "&pn=1&nohot=1&sort=0"
    r = requests.get(url, headers={'User-Agent': user_agent}, timeout=30)
    numtext = r.text
    json_text = json.loads(numtext)
    print(json_text['code'] == 12002)
    if json_text['code'] != 12002:
        commentsNum = json_text["data"]["page"]["count"]
        page = commentsNum // 20 + 2
        print(page)
        if page >= 4:
            num = []
            pj = page // 4
            for i in range(4):
                s = pj * i
                num.append(s)
            print(num)
            q = mp.Queue()    
            for i in range(4):
                p = mp.Process(target=get_reply, args=(num[i], pj, q, aid))
                p.start()
            reply = []
            for i in range(4):
                # time.sleep(3)
                # print(q.empty())
                while q.empty():
                    # print('等待第', i, '次')
                    time.sleep(2)
                    continue
                reply = reply + q.get()
            print(reply)
            return reply
        else:
            pj = page
            for i in range(page):
                num.append(i)
            for i in range(page):
                q = mp.Queue()
                p = mp.Process(target=get_reply, args=(num[i], 1, q, aid))
                p.start()
            reply = []
            for i in range(page):
                while q.empty():
                    continue
                reply.append(q.get())
            return reply

    else:
        return None


if __name__ == '__main__':
    VedioList('777536')