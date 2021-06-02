import time
from google.cloud import storage
import os
from pathlib import Path
from concurrent import futures
client = storage.Client()
bucket = client.bucket("transsion-poc-0522")
MaxThread = 10
MaxFile = 4
MB = 1024*1024
chunksize = 10*MB


def upload_file(file_name):
    blob = bucket.blob(file_name)
    _path = Path.cwd() / file_name
    #print(_path)
    blob.upload_from_filename(_path)


def download_file(file_name):

    blob = bucket.get_blob(file_name)
    file_name = Path('/tmp') / file_name
    blob.download_to_filename(file_name)


def upload_file_parallel(file_name):
    try: 
        file_size = os.path.getsize(file_name)
        if file_size < chunksize:  # Less than 10MB, direct call
            upload_file(file_name)
        else:
            # Larger than 10MB, multi-threads
            def split(file_size, chunksize):
                index_list = [0]  # File reading index number (bytes)
                partnumber = 1
                while chunksize * partnumber < file_size:
                    index_list.append(chunksize * partnumber)
                    partnumber += 1
                return index_list
            
            def upload_chunk(file_name, file_start, file_end, chunk_path):
                try:
                    blob = bucket.blob(chunk_path)
                    with open(file_name, "rb") as data:
                        data.seek(file_start)
                        chunkdata = data.read(file_end - file_start + 1)
                        blob.upload_from_string(chunkdata)
                except Exception as e:
                    print(e)
                return

            def compose(name_list, file_name):
                com_list = []  # List for composing
                del_list = []  # List to delete
                com_size = 32  # compose bucket max size
                com_number = 0  # temp name for composed file
                for b in name_list:
                    blob_src = bucket.blob(b)
                    com_list.append(blob_src)
                    # if com_list reach com_size or list end, then compose
                    if len(com_list) == com_size or b == name_list[-1]:
                        if b == name_list[-1]:
                            com_name = file_name
                        else:
                            com_name = f"{file_name}.x-com-{com_number}"
                            com_number += 1
                        blob_com = bucket.blob(com_name)
                        blob_com.compose(com_list, timeout=600)
                        del_list.extend(com_list)
                        com_list = [blob_com]
                for blob in del_list:
                    blob.delete()
                return

            index_list = split(file_size, chunksize)
            chunk_list = []
            with futures.ThreadPoolExecutor(max_workers=MaxThread) as pool:
                for file_start in index_list:
                    chunk_path = f"{file_name}.x-tmp-{str(file_start)}"
                    chunk_list.append(chunk_path)
                    if file_start + chunksize >= file_size:
                        file_end = file_size - 1
                    else:
                        file_end = file_start + chunksize - 1
                    pool.submit(upload_chunk, file_name, file_start, file_end, chunk_path)
            compose(chunk_list, file_name)
    except Exception as e:
        print(e)
    return


def download_file_parallel(file_name):
    try:
        file_size = os.path.getsize(file_name)
        if file_size < chunksize:  # Less than 10MB, direct call
            download_file(file_name)
        else:
            # Larger than 10MB, multi-threads
            def split(file_size, chunksize):
                index_list = [0]  # File reading index number (bytes)
                partnumber = 1
                while chunksize * partnumber < file_size:
                    index_list.append(chunksize * partnumber)
                    partnumber += 1
                return index_list
            
            def download_chunk(file_start, file_end, file_name, wfile):
                try:
                    blob = bucket.blob(file_name)
                    chunkdata = blob.download_as_bytes(
                                start=file_start, end=file_end,
                                raw_download=True, checksum=None)
                    wfile.seek(file_start)
                    wfile.write(chunkdata)
                except Exception as e:
                    print(e)
                return

            index_list = split(file_size, chunksize)
            tmp_name = file_name + ".x-gcs-tmp"
            with open(tmp_name, "wb") as wfile:
                with futures.ThreadPoolExecutor(max_workers=MaxThread) as pool:
                    for file_start in index_list:
                        if file_start + chunksize >= file_size:
                            file_end = file_size - 1
                        else:
                            file_end = file_start + chunksize - 1
                        pool.submit(download_chunk, file_start, file_end, file_name, wfile)
            os.rename(tmp_name, file_name)
    except Exception as e:
        print(e)
    return


def main():
    write('Start')
    for root,dirs,files in os.walk(Path.cwd()):
        print(files)
        elaspeds = []
        for i in range(1,2):  # TODO: 11
            start = int(round(time.time()*1000))
            with futures.ThreadPoolExecutor(max_workers=MaxFile) as pool:
                for file in files:
                    # pool.submit(upload_file_parallel,file)
                    pool.submit(download_file_parallel,file)
            end = int(round(time.time()*1000))
            elasped = end-start
            elaspeds.append(elasped)
            write('第{}次用时:{}ms'.format(i,elasped))
        elaspeds.remove(min(elaspeds))
        elaspeds.remove(max(elaspeds))
        avg_results = float(sum(elaspeds)) / len(elaspeds)
        write('平均用时:{}ms'.format(avg_results))


def write(messsage):
    with open('/data/gcp_oss_0602.txt','a+') as fd:
        fd.write(messsage+'\n')


def file_total_size(path,files):
    total_size = 0
    for file in files:
        total_size += os.stat(os.path.join(path,file)).st_size
    write('文件总大小为:{}'.format(total_size))


if __name__ == '__main__':
    main()
