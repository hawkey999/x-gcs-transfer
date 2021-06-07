import time
import boto3
import os
from pathlib import Path
from concurrent import futures
from boto3.s3.transfer import TransferConfig
MaxThread = 16
MaxFile = 16
s3_resource = boto3.resource('s3', endpoint_url="https://storage.googleapis.com")
config = TransferConfig(multipart_threshold=1024 * 1024 * 8, 
    max_concurrency=MaxThread, 
    multipart_chunksize=1024 * 1024 * 8,
    use_threads=True)
bucket = "transsion-poc-0522"


def multipart_upload_boto3(file_name):
    try:
        s3_resource.Object(bucket, file_name).upload_file(file_name, Config=config)
    except Exception as e:
        print(e)


def main():
    write('Start')
    for root,dirs,files in os.walk(Path.cwd()):
        elaspeds = []
        for i in range(1,11):
            start = int(round(time.time()*1000))
            with futures.ThreadPoolExecutor(max_workers=MaxFile) as pool:
                for file in files:
                    pool.submit(multipart_upload_boto3,file)
            end = int(round(time.time()*1000))
            elasped = end-start
            elaspeds.append(elasped)
            write('第{}次用时:{}ms'.format(i,elasped))
        elaspeds.remove(min(elaspeds))
        elaspeds.remove(max(elaspeds))
        avg_results = float(sum(elaspeds)) / len(elaspeds)
        write('平均用时:{}ms'.format(avg_results))


def write(messsage):
    with open('/data/ggcp_hmac.txt','a+') as fd:
        fd.write(messsage+'\n')


def file_total_size(path,files):
    total_size = 0
    for file in files:
        total_size += os.stat(os.path.join(path,file)).st_size
    write('文件总大小为:{}'.format(total_size))


if __name__ == '__main__':
    main()
