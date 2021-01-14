# Google Cloud Storage strong upload/download
Google Cloud Storage 大文件上传下载或恶劣网络传输场景

## 使用说明
1. 认证（三选一）
 - 如果运行在GCE，则配置IAM Service Account给GCE，并且GCE的访问API权限至少要有Storage和oauth两个API权限（可以全打开，只由IAM控制）
 - 在本地电脑运行，并曾经配置过gcloud命令行，并进行了初始化和认证，可以运行gsutil的环境  
 - 通用方式。在IAM Service Account下载Json Key，并配置环境变量指向这个Key文件  
    export GOOGLE_APPLICATION_CREDENTIALS="/home/user/Downloads/my-key.json"  
    参见：https://cloud.google.com/docs/authentication/production

2. 安装依赖包  
```
pip3 install -r requirements.txt
```

3. 简单运行  
```
python3 gcs-xfile-transfer.py \
--job_type download \
--local_dir "/home/my_user/download/" \
--bucket my_bucket_name
```
高级选项  
```
python3 gcs-xfile-transfer.py \
--job_type download \
--local_dir "/home/my_user/download/" \
--bucket my_bucket_name \
--prefix "prefix_in_the_bucket" \
--temp_prefix x-gcs-temp \
--max_concurrent_files 3 \
--max_concurrent_threads_per_file 5 \
--conn_timeout 3 \
--read_timeout 30 \
--max_retry 300
```

下载示例：
```
python3 gcs-xfile-transfer.py \
--job_type download \
--local_dir "/Users/hzb/Downloads/test_folder" \
--bucket lab-hzb-us-central1 \
--prefix "x-gcs-upload/"
```
上传示例：
```
python3 gcs-xfile-transfer.py \
--job_type upload \
--local_dir "/Users/hzb/Downloads/test_folder" \
--bucket lab-hzb-us-central1 \
--conn_timeout 5 \
--read_timeout 60
```
