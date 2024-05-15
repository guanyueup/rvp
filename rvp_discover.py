import os
import pandas as pd
import argparse
from S3BucketUtil import S3Bucket


user_home = os.path.expanduser('~')
project_path = user_home + '/rvp_discover'
s3_bucket = S3Bucket()

S3_FILE_CONF = {
    "ACCESS_KEY": "F75b8o8HBgVtGzW7",
    "SECRET_KEY": "Mn1bOifJ8u6WnyoeV1sWcqv7UmIxvAJW",
    "BUCKET_NAME": "kdp",
    "ENDPOINT_URL": "http://166.111.121.63:59000/",
    
    "DOWN_S3_DIR": "/ris_v6_prefix/bgp_prefix.csv" ,   #需要下载的s3的文件目录",
    "DOWN_LOCAL_DIR":project_path,  #本地目录",

    "UPLOAD_S3_DIR": "/rvp_data/", #需要上传的s3路径
    "UPLOAD_FILE_DIR": "./test.txt",  #需要上传的本地文件

}
down_s3_dir = S3_FILE_CONF["DOWN_S3_DIR"]
down_local_dir = S3_FILE_CONF["DOWN_LOCAL_DIR"]
s3_bucket.download_file(down_s3_dir, down_local_dir)

csv_row_data = pd.read_csv(project_path+'/bgp_prefix.csv',header=None)
prefixs = list(csv_row_data[0])

def path_exists(path):
    if not os.path.exists(path):
        os.mkdir(path)

def filterPrefixs(prefix, ban_len):
    l = prefix.split('/')
    if int(l[1]) < ban_len: return True
    return False

def parse_opt():
    parser = argparse.ArgumentParser()
    parser.add_argument('--start',type=int,default=1 ,help='扫描的起始位置，包含当前值。')
    parser.add_argument('--end',type=int,default = -1,help='扫描的结束位置，包含当前值,-1表示到最后一个值。')
    parser.add_argument('--blen',type=int,default = 40, help='忽视前缀的长度，len = 40 表示前缀小于/40的不进行扫描，不包含40')
    parser.add_argument('--nc',type=str,help='选择网卡。')
    parser.add_argument('--slen',type=int,default=64, help='扫描子网的最大范围，slen=64表示扫描到/64子网。')
    parser.add_argument('--pps',type=int    ,default=24500,help='每秒发包速率')
    return parser.parse_args()

if __name__ == '__main__':
    args = parse_opt()
    start = args.start
    end = args.end
    subnetprefix = args.slen
    ban_len = args.blen
    networkCard = args.nc
    pps = args.pps
    if end == -1: end = len(prefixs)

    for i,prefix in enumerate(prefixs):
        if i + 1 < start or filterPrefixs(prefix,ban_len):
            if i + 1 == end: break
            continue 
        outfields = r'--output-fields="saddr,outersaddr,clas,desc,"'
        save_path = project_path + f'/rvp_data/result{i+1}.csv'
        command = f'xmap -6 -x {subnetprefix} {prefix} -R {pps} -M icmp_echo_gw -i {networkCard} -G ee:ff:ff:ff:ff:ff -U rand -O csv -output-filter="success = 1 || success = 0" {outfields} -o {save_path}'
        rcode = os.system(command)
        if rcode != 0:
            raise SystemError('The xmap excution failed!')
        # upload_s3_dir = S3_FILE_CONF["UPLOAD_S3_DIR"]
        # upload_local_dir = save_path
        # s3_bucket.upload_normal(upload_s3_dir, upload_local_dir)
        # os.remove(upload_local_dir)
        if i+1 == end: break
