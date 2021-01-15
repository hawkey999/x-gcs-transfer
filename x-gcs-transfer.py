from google.cloud import storage
from google.api_core import retry as google_retry
from concurrent import futures
from pathlib import PurePosixPath, Path
import os
import sys
import json
import time
import datetime
import argparse
import sqlite3
import uuid
import mimetypes
from tenacity import retry, wait_random, stop_after_attempt, before_sleep_log
import logging


parser = argparse.ArgumentParser()
parser.add_argument("--job_type", type=str)
parser.add_argument("--local_dir", type=str)
parser.add_argument("--bucket", type=str)
parser.add_argument("--prefix", type=str, default="")
parser.add_argument("--temp_prefix", type=str, default="x-gcs-temp")
parser.add_argument("--max_concurrent_files", type=int, default=3)
parser.add_argument("--max_concurrent_threads_per_file", type=int, default=5)
parser.add_argument("--conn_timeout", type=int, default=5)
parser.add_argument("--read_timeout", type=int, default=60)
parser.add_argument("--max_retry", type=int, default=300)
parser.add_argument("--chunksize", type=int, default=5)

args = parser.parse_args()
bucket_name = args.bucket
bucket_prefix = PurePosixPath(args.prefix)
if args.prefix == "/" or args.prefix == ".":
    bucket_prefix = PurePosixPath("")
temp_path = bucket_prefix / args.temp_prefix
local_dir = args.local_dir
job_type = args.job_type.lower()
MB = 1024*1024
min_split_size = 10*MB  # If file size less than this, then use original google lib to upload
chunksize = args.chunksize*MB  # Set default chunksize
MaxThread = args.max_concurrent_threads_per_file  # Concurrent loading threads for each file.
MaxFile = args.max_concurrent_files
# Concurrent loading files. Threads x Files x chunksize should be less than available memory
tranport_timeout = (args.conn_timeout, args.read_timeout)
# Tuple (connect_timeout = conn setup time, read_timeout = time to transfer one chunk)
MaxRetry = args.max_retry  # retries on a single request of upload/download

storage_client = storage.Client()
bucket = storage_client.bucket(bucket_name)
g_retry = google_retry.Retry(deadline=600)  # retry timeout on composing

logger = logging.getLogger()
streamHandler = logging.StreamHandler()
streamHandler.setFormatter(logging.Formatter('%(asctime)s %(levelname)s - %(message)s'))
logger.addHandler(streamHandler)
logger.setLevel(logging.INFO)
# TODO: export log to file


def split(file_size, c_size):
    index_list = [0]  # File reading index number (bytes)
    partnumber = 1
    while c_size * partnumber < file_size:
        index_list.append(c_size * partnumber)
        partnumber += 1
    return index_list


@retry(wait=wait_random(min=1, max=3), reraise=True, stop=stop_after_attempt(MaxRetry),
       before_sleep=before_sleep_log(logger, logging.WARN))
def upload_func(blob, chunkdata, rel_path, file_progress):
    # logger.info(f"-Start upload Chunk:{loaded_name} ")
    content_type = mimetypes.guess_type(os.path.basename(rel_path))[0]
    timestart = time.time()
    blob.upload_from_string(chunkdata,
                            content_type=content_type,
                            timeout=tranport_timeout,
                            checksum="md5"
                            )
    timespent = int((time.time() - timestart) * 1000) / 1000
    pload_bytes = len(chunkdata)
    speed = size_to_str(int(pload_bytes / timespent)) + "/s"
    logger.info(f"-Upload Chunk:{rel_path} Time:{timespent}s Speed:{speed} FileProgress:{file_progress}%")
    return


# convert bytes to human readable string
def size_to_str(size):
    def loop(integer, remainder, level):
        if integer >= 1024:
            remainder = integer % 1024
            integer //= 1024
            level += 1
            return loop(integer, remainder, level)
        else:
            return integer, round(remainder / 1024, 1), level
    units = ['B', 'KB', 'MB', 'GB', 'TB', 'PB']
    integer, remainder, level = loop(int(size), 0, 0)
    if level+1 > len(units):
        level = -1
    return f'{integer+remainder} {units[level]}'


def upload_chunk(rel_path, file_start, file_end, chunk_path, file_progress):
    abs_path = str(Path(local_dir) / rel_path)
    try:
        blob = bucket.blob(chunk_path)
        # if blob.exists():
        #     logger.info(f"-Chunk exists, skip to next! Chunk:{loaded_name} FileProgress:{file_progress}%")
        #     return
        with open(abs_path, "rb") as data:
            data.seek(file_start)
            chunkdata = data.read(file_end - file_start + 1)
        upload_func(blob, chunkdata, rel_path, file_progress)
    except Exception as e:
        logger.error(f"Error while uploading chunk:{rel_path}, reason:{e}, {upload_func.retry.statistics}")
        sys.exit(0)
    return


def compose(name_list, rel_path):
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
                com_name = str(bucket_prefix / rel_path)
            else:
                com_name = f"{str(temp_path / rel_path)}.x-com-{com_number}"
                com_number += 1
            blob_com = bucket.blob(com_name)
            try:
                blob_com.compose(com_list, timeout=600)
            except Exception as e:
                logger.error(f"Error while composing {rel_path}, {e}")
                sys.exit(0)
            del_list.extend(com_list)
            # TODO: re-caculate crc32 of local file to compare, higher compute cost
            com_list = [blob_com]  # Add the composed object as the next composing list's first object
    # Delete the delete_list
    for blob in del_list:
        blob.delete()
    logger.info(f"-Complete compose! File: {rel_path}")
    return


@retry(wait=wait_random(min=1, max=3), reraise=True, stop=stop_after_attempt(MaxRetry),
       before_sleep=before_sleep_log(logger, logging.WARN))
def upload_small(local_path, rel_path):
    blob_name = str(bucket_prefix / rel_path)
    blob = bucket.blob(blob_name)
    blob.upload_from_filename(local_path, timeout=tranport_timeout)
    logger.info(f"-Complete upload small file: {rel_path}")
    return


def upload_file(upload_job):
    rel_path = upload_job["File_relPath"]
    abs_path = Path(local_dir)/rel_path
    file_size = upload_job["Size"]
    if file_size < min_split_size:
        try:
            upload_small(str(abs_path), rel_path)
        except Exception as e:
            logger.error(f"Error while uploading {rel_path}, reason:{e}, {upload_small.retry.statistics}")
            sys.exit(0)
        return

    index_list = split(file_size, chunksize)
    chunk_list = []
    # Get file chunk list exist on bucket tmp
    c_object_list = list_bucket(prefix=f"{str(temp_path / rel_path)}.x-tmp-", only_list_file=True)

    with futures.ThreadPoolExecutor(max_workers=MaxThread) as pool:
        for file_start in index_list:
            chunk_path = f"{str(temp_path / rel_path)}.x-tmp-{str(file_start)}"
            chunk_list.append(chunk_path)
            if file_start + chunksize >= file_size:
                file_end = file_size - 1
            else:
                file_end = file_start + chunksize - 1
            file_progress = int((file_end + 1) / file_size * 10000) / 100
            if chunk_path not in c_object_list:  # Check chunk exist
                pool.submit(upload_chunk, rel_path, file_start, file_end, chunk_path, file_progress)
            else:
                logger.info(f"-Skip upload Chunk:{rel_path} FileProgress:{file_progress}%")
    compose(chunk_list, rel_path)
    return


def list_local(path):
    file_list = []
    for parent, dirnames, filenames in os.walk(path):
        for filename in filenames:
            file_absPath = os.path.join(parent, filename)
            file_relativePath = file_absPath[len(path) + 1:]
            file_size = os.path.getsize(file_absPath)
            file_list.append({
                "File_relPath": file_relativePath,
                "Size": file_size
            })
    return file_list


def list_bucket(prefix, only_list_file=False):
    object_list = []

    if prefix != ".":
        blobs = storage_client.list_blobs(bucket_name, prefix=prefix, timeout=tranport_timeout, retry=g_retry)
    else:  # If no prefix (i.e. prefix=".") then cannot add prefix "" in the request
        blobs = storage_client.list_blobs(bucket_name, timeout=tranport_timeout, retry=g_retry)
    for page in blobs.pages:
        for blob in page:
            absPath = blob.name
            size = blob.size
            if prefix != "." and prefix != absPath:
                relPath = absPath[len(prefix) + 1:]
            else:
                relPath = absPath
            if only_list_file:
                object_list.append(absPath)
            else:
                object_list.append({
                    "File_relPath": relPath,
                    "Size": size
                })
    return object_list


def compare_upload(path, prefix_str):
    local_list = list_local(path)
    logger.info(f"Listing gs://{PurePosixPath(bucket_name) / prefix_str}")
    bucket_obj_list = list_bucket(prefix_str)
    delta_list = []
    for i in local_list:
        if i not in bucket_obj_list:
            delta_list.append(i)
    return delta_list


def compare_download(prefix_str, path):
    local_list = list_local(path)
    logger.info(f"Listing gs://{PurePosixPath(bucket_name) / prefix_str}")
    bucket_obj_list = list_bucket(prefix_str)
    delta_list = []
    for i in bucket_obj_list:
        if i not in local_list:
            delta_list.append(i)
    return delta_list


@retry(wait=wait_random(min=1, max=3), reraise=True, stop=stop_after_attempt(MaxRetry),
       before_sleep=before_sleep_log(logger, logging.WARN))
def download_small(local_path, rel_path):
    blob_name = str(bucket_prefix / rel_path)
    blob = bucket.blob(blob_name)
    blob.download_to_filename(local_path, timeout=tranport_timeout)
    logger.info(f"-Complete download small file: {rel_path}")
    return


def create_dir(file_dir):
    parent = file_dir.parent
    if not Path.exists(parent):
        create_dir(parent)
    try:
        Path.mkdir(file_dir)
    except Exception as _e:
        logger.error(f'Fail to mkdir {str(_e)}')
    return


def download_file(download_job):
    rel_path = download_job["File_relPath"]
    abs_path = Path(local_dir) / rel_path
    file_size = download_job["Size"]

    # Create parent dir
    path = abs_path.parent
    if not Path.exists(path):
        create_dir(path)

    # Create and skip download if it is "/" subfolder
    if rel_path[-1] == "/":
        Path.mkdir(abs_path)
        logger.info(f"Create empty sub-folder: {abs_path}")
        return

    # Download small file
    if file_size < min_split_size:
        try:
            download_small(str(abs_path), rel_path)
        except Exception as e:
            logger.error(f"Error while downloading {rel_path}, reason:{e}, {download_small.retry.statistics}")
            sys.exit(0)
        return

    # Get file chunk list exist on db
    chunk_list = []
    try:
        with sqlite3.connect("_x-gcs_download.db") as db:
            cursor = db.cursor()
            p_sql = cursor.execute(
                f"SELECT CHUNK_INDEX FROM GCSC WHERE BUCKET='{bucket_name}' AND KEY='{abs_path.as_uri()}'")
            db.commit()
            chunk_list = [d[0] for d in p_sql]
    except Exception as e:
        logger.info(f"Can't read resumable chunk index from sqlite3 DB. {e}")
        # Keep the chunk_list empty

    # download chunked big file
    index_list = split(file_size, chunksize)
    tmp_name = abs_path.with_suffix(".x-gcs-tmp")
    if Path.exists(tmp_name):
        mode = 'r+b'
    else:
        # there is no temp file, then create empty file and empty chunk_list
        mode = "wb"
        chunk_list = []
    with open(tmp_name, mode) as wfile:
        with futures.ThreadPoolExecutor(max_workers=MaxThread) as _pool:
            for file_start in index_list:
                if file_start + chunksize >= file_size:
                    file_end = file_size - 1
                else:
                    file_end = file_start + chunksize - 1
                file_progress = int((file_end + 1) / file_size * 10000) / 100
                if file_start not in chunk_list:
                    _pool.submit(download_chunk, file_start, file_end, rel_path, wfile, file_progress)
                else:
                    logger.info(f"-Skip download Chunk:{rel_path} FileProgress:{file_progress}%")

    # Complete local file, clean index db
    tmp_name.rename(abs_path)
    try:
        with sqlite3.connect("_x-gcs_download.db") as db:
            cursor = db.cursor()
            cursor.execute(
                f"DELETE FROM GCSC WHERE BUCKET='{bucket_name}' AND KEY='{abs_path.as_uri()}'")
            db.commit()
    except Exception as e:
        logger.error(f"Fail to delete chunk index from sqlite3 DB. {e}")
    logger.info(f"-Complete compose! File {rel_path}")
    return


@retry(wait=wait_random(min=1, max=3), reraise=True, stop=stop_after_attempt(MaxRetry),
       before_sleep=before_sleep_log(logger, logging.WARN))
def download_func(blob, file_start, file_end, rel_path, file_progress):
    timestart = time.time()
    response = blob.download_as_bytes(
        start=file_start, end=file_end,
        raw_download=True, timeout=tranport_timeout, checksum=None)  # TODO: Check local crc32 with remote object
    timespent = int((time.time() - timestart) * 1000) / 1000
    pload_bytes = len(response)
    speed = size_to_str(int(pload_bytes / timespent)) + "/s"
    logger.info(
        f"-Downloaded Chunk:{rel_path} Time:{timespent}s Speed:{speed} FileProgress:{file_progress}%")
    return response


def download_chunk(file_start, file_end, rel_path, wfile, file_progress):
    abs_path = Path(local_dir) / rel_path
    try:
        blob = bucket.blob(str(bucket_prefix/rel_path))
        chunkdata = download_func(blob, file_start, file_end, rel_path, file_progress)
        wfile.seek(file_start)
        wfile.write(chunkdata)
    except Exception as e:
        logger.error(f"Error download chunk:{rel_path}, reason:{e}, {download_func.retry.statistics}")
        sys.exit(0)
    try:
        with sqlite3.connect("_x-gcs_download.db") as db:
            cursor = db.cursor()
            uuid1 = uuid.uuid1()
            cursor.execute(f"INSERT INTO GCSC (ID, BUCKET, KEY, CHUNK_INDEX) "
                           f"VALUES ('{uuid1}', '{bucket_name}', '{abs_path.as_uri()}', {file_start})")
            db.commit()
    except Exception as e:
        logger.error(f"Error write chunk index to sqlite3 DB. {e}")
    return


def create_db():
    with sqlite3.connect('_x-gcs_download.db') as db:
        cursor = db.cursor()
        cursor.execute("CREATE TABLE IF NOT EXISTS GCSC "
                       "(ID TEXT PRIMARY KEY, "
                       "BUCKET TEXT, "
                       "KEY TEXT, "
                       "CHUNK_INDEX INTEGER)")
        db.commit()
    return


if __name__ == '__main__':
    start_time = datetime.datetime.now()
    logger.info(f"Comparing {local_dir} and {bucket_name}/{bucket_prefix}, job_type {job_type}")
    if job_type == "upload":
        job_list = compare_upload(local_dir, str(bucket_prefix))
    elif job_type == "download":
        # Define DB table to save resumable chunk index
        create_db()
        job_list = compare_download(str(bucket_prefix), local_dir)
    else:
        logger.error(f"Unsupport job_type: {job_type}")
        sys.exit(0)

    logger.info(f"Listed {len(job_list)} objects to {job_type}")
    # Submit threads pool for multi-files concurrent
    with futures.ThreadPoolExecutor(max_workers=MaxFile) as pool:
        for job in job_list:
            if job_type == "upload":
                pool.submit(upload_file, job)
            elif job_type == "download":
                pool.submit(download_file, job)

    time_str = str(datetime.datetime.now() - start_time)
    logger.info(f"All files completed. Time:{time_str}. "
                f"Comparing {local_dir} and {bucket_name}/{bucket_prefix}, job_type {job_type}")
    # Compare list again
    if job_type == "upload":
        job_list = compare_upload(local_dir, str(bucket_prefix))
    elif job_type == "download":
        job_list = compare_download(str(bucket_prefix), local_dir)

    if job_list:  # job_list is not null then warning delta list
        logger.warning(f"Delta list: {json.dumps(job_list, indent=4)}")
    else:
        logger.info("List match, no delta objects to transfer")

