# -*- coding: utf-8 -*-
"""
Created on Tue Sep 12 23:51:53 2023

@author: ChenYang
"""
import os
import oss2
import time
from tqdm import tqdm

# 首先初始化AccessKeyId、AccessKeySecret、Endpoint等信息。
# 通过环境变量获取，或者把诸如“<你的AccessKeyId>”替换成真实的AccessKeyId等。
#
# 以杭州区域为例，Endpoint可以是：
#   http://oss-cn-hangzhou.aliyuncs.com
#   https://oss-cn-hangzhou.aliyuncs.com
# 分别以HTTP、HTTPS协议访问。
access_key_id = os.getenv('OSS_TEST_ACCESS_KEY_ID', 'your key')
access_key_secret = os.getenv('OSS_TEST_ACCESS_KEY_SECRET', 'your secret')
bucket_name = os.getenv('OSS_TEST_BUCKET', 'your bucket name')
endpoint = os.getenv('OSS_TEST_ENDPOINT', 'your endpoint')


# 确认上面的参数都填写正确了
for param in (access_key_id, access_key_secret, bucket_name, endpoint):
    assert '<' not in param, '请设置参数：' + param

# 创建Bucket对象，所有Object相关的接口都可以通过Bucket对象来进行
bucket = oss2.Bucket(oss2.Auth(access_key_id, access_key_secret), endpoint, bucket_name)

num = 0
f_set = set() #初始化空文件集合
while 1:
    num += 1
    temp_set = set(obj.key for obj in oss2.ObjectIteratorV2(bucket, prefix='djiM300media/DJI')) #获取文件集合 prefix为文件夹名及文件前缀
    print("第{}次获取文件集合".format(num))
    dis = temp_set - f_set #同已有文件集合作差
    dis_sort = sorted(dis)
    if dis:
        f_set = temp_set #更新已有集合
        print("文件集合已更新，正在下载文件")
        for name in tqdm(dis_sort): #差集中的文件逐个下载
            key = name #获取集合中的文件名
            folder, filename = name.split('/') #命名本地文件名77  
            # 下载文件
            result = bucket.get_object(key)
            # 下载到本地文件
            result = bucket.get_object_to_file(key, 'temp/'+ filename)
            os.rename('temp/'+ filename, folder + '/' + filename)
        print("新增文件下载完成\n")
    else:
        print("无新增文件\n")
    time.sleep(2) #2秒后再次访问aliyun

#key = 'djiM300media/DJI_20230912133942_0001_D.JPG'
#filename = 'download.jpg'
