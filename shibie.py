from mysql import connector
import redis
import csv
import os
import jieba.analyse
import re


###Mysql和redis连接
# 连接redis
rd = redis.Redis(host='127.0.0.1', port=6379)
# 连接mysql
db = connector.connect(host='localhost', user='gaojunbin', passwd='18759799353', database='bilibili')
# 获取游标
yb = db.cursor()


# 查找所有的标签记录
yb.execute('select title, tags, danmu, comment from video where uid = 777536;')
# print(yb.fetchall())
# 创建一个csv文件
with open('蕾丝.csv', 'w', newline='')as f:
	header = ('标题', '标签', '频繁出现词语', '是否出现指定词语', '识别结果')
	# mysql 查询结果
	jieguo = yb.fetchall()
	f_csv = csv.writer(f)
	f_csv.writerow(header)
	# 结果写到csv文件
	for index, res in enumerate(jieguo):
		### 初始化变量
		discover = '否'  # 是否出现指定词语
		result = '无'  # 识别结果
		hang = ()
		title = res[0]  # 标题
		# 获取标签列表
		tags_data = res[1]
		tags = re.split(r"\+\+", tags_data)[1:]
		print(tags)
		# 获取弹幕列表
		danmu_data = res[2]
		danmu = re.split(r"\+\+", danmu_data)[1:]
		# 获取评论列表
		comment_data = res[3]
		comment = re.split(r"\+\+", comment_data)[2:]
		# 将弹幕和评论写入文件
		with open(str(index) + '.xml', 'w')as x:
			for i in danmu:
				x.writelines(i)
			for j in comment:
				x.writelines(j)
		# 用弹幕和评论文件查找最频繁出现的词
		content = open(str(index) + '.xml', 'rb').read()
		frequently = jieba.analyse.extract_tags(content, topK=30)
		# print(frequently)
		# 删除文件
		os.remove(str(index) + '.xml')
		# 查看是否出现指定词语
		if '恰饭' in frequently or '发电' in frequently:
			discover = '是'
		# 查看标签是否在游戏列表内
		for word in tags:
			if rd.get(word) is not None:
				result = word
				# print(result)
		# 将结果写到csv文件
		hang = (title, str(tags), str(frequently), discover, result)
		print(hang)
		f_csv.writerow(hang)


###Mysql和redis关闭
# 关闭Mysql数据库和游标
yb.close()
db.close()
# 关闭redis连接
# rd.close()