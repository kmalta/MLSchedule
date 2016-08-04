import numpy as np
import os
import sys
from subprocess import Popen, PIPE
from time import sleep
import math

#Dependency Files
import glob
from deploy_cloud import *

local_split = 'gsplit'

def chunk_data_locally(file_bucket_path, chunk_bucket_path, filename, chunksize):
    py_cmd_line('find ./temp_data_chunks -maxdepth 1 -name "*" -print0 | xargs -0 rm')

    if file_bucket_path == 'local':
        py_s3cmd_mb(chunk_bucket_path.split('/')[-1])
    else:
        py_s3cmd_get(file_bucket_path + '/' + filename)
        py_cmd_line('mv ' + filename + ' ' + glob.DATA_SET_PATH)

    print glob.DATA_SET_PATH
    stdout = py_out_proc('wc -l ' + glob.DATA_SET_PATH + '/' + filename)

    num_lines = int(stdout.split()[0])
    
    stdout = py_out_proc('ls -l ' + glob.DATA_SET_PATH + '/' + filename)
    print stdout
    file_byte_size = int(stdout.split()[4])
    print file_byte_size
    print num_lines
    num_chunks = int(math.ceil(float(file_byte_size)/chunksize))
    print num_chunks

    num_digits = int(math.ceil(math.log(num_chunks, 10)))

    py_cmd_line('mkdir temp_data_chunks')
    py_cmd_line(local_split + ' -n l/' + str(num_chunks) + ' -a ' + str(num_digits)
                + ' -d ' + glob.DATA_SET_PATH + '/' + filename + ' temp_data_chunks/' + filename + '-chunk-')

    stdout = py_out_proc('ls -l ' + 'temp_data_chunks/' + filename + '-chunk-' + num_digits*"0")
    chunk_byte_size = int(stdout.split()[4])


    metadata_filepath = 'temp_data_chunks/' + filename + '-chunk-metadata'

    f = open(metadata_filepath, 'w')
    f.write('file_byte_size' + '\t' + 'num_chunks' + '\t'
            + 'num_digits' + '\t' + 'chunk_byte_size' + '\n')
    f.write(str(file_byte_size) + '\t' + str(num_chunks) + '\t'
            + str(num_digits) + '\t' + str(chunk_byte_size) + '\n')


    py_s3cmd_put(metadata_filepath, chunk_bucket_path)

    for i in range(num_chunks):
        num_zeros = num_digits - len(str(i))
        filepath_to_upload = 'temp_data_chunks/' + filename + '-chunk-' + num_zeros*"0" + str(i)
        py_s3cmd_put(filepath_to_upload, chunk_bucket_path)


def delete_chunks(chunk_bucket_path, filename):
    os.system('s3cmd -c s3cfg del ' + chunk_bucket_path + '/' + filename + '-chunk-*')

def doll_out_chunks(num_chunks, machine_cores_array):
    num_machs = len(machine_cores_array)
    total_cores = sum(machine_cores_array)
    percentage_cores_array = [float(i)/total_cores for i in machine_cores_array]
    dolled_out_chunks = [int(math.floor(float(i*num_chunks))) for i in percentage_cores_array]
    remainder = num_chunks - sum(dolled_out_chunks)
    for j in range(remainder):
        temp_arr = [float(dolled_out_chunks[i]+ 1)/machine_cores_array[i] for i in range(num_machs)]
        dolled_out_chunks[temp_arr.index(min(temp_arr))] += 1

    return dolled_out_chunks


def assign_data_chunks(chunk_partitions, num_digits):
    num_chunks = sum(chunk_partitions)
    chunk_ranges = []
    idx = 0
    for chunks in chunk_partitions:
        range_arr = []
        for i in range(chunks):
            range_arr.append((num_digits - len(str(idx)))*'0' + str(idx))
            idx += 1
        chunk_ranges.append(range_arr)

    return chunk_ranges

def replace_s3cfg(ip):
    ip_s3 = py_out_proc('dig +short ' + glob.REGION).strip()
    print "@@@@@@@@", ip_s3
    with open(glob.S3CMD_CFG_PATH, 'r') as input_file, open(glob.S3CMD_CFG_PATH + '-temp', 'w') as output_file:
        for line in input_file:
            if 'host_base' in line:
                output_file.write('host_base = ' + ip_s3 + ':8773\n')
            elif 'host_bucket' in line:
                output_file.write('host_bucket = ' + ip_s3 + ':8773\n')
            else:
                output_file.write(line)

    py_cmd_line('mv ' + glob.S3CMD_CFG_PATH + '-temp ' + glob.S3CMD_CFG_PATH)
    py_scp_to_remote('', ip, glob.S3CMD_CFG_PATH, glob.REMOTE_CFG)
    return

def distribute_chunks(machine_array, data_chunk_bucket_path, data_name):

    py_cmd_line('rm *-chunk-metadata')
    data_path = data_chunk_bucket_path + '/' + data_name
    py_s3cmd_get(data_path + '-chunk-metadata')
    py_cmd_line('mv ' + data_name + '-chunk-metadata ' + glob.DATA_SET_PATH)
    f = open(glob.DATA_SET_PATH + '/' + data_name + '-chunk-metadata', 'r')
    data = f.readlines()[1].split()
    num_chunks = int(data[1])
    num_digits = int(data[2])
    f.close()


    machine_cores_array = get_machine_cores_from_names(machine_array)
    chunk_partitions = doll_out_chunks(num_chunks, machine_cores_array)

    chunk_ranges = assign_data_chunks(chunk_partitions, num_digits)

    f2 = open('hostfile', 'r')
    host_ips = f2.readlines()
    for i in range(len(chunk_ranges)):
        ip = host_ips[i].strip()
        with open('chunks-' + str(i), 'w') as f:
            for idx in chunk_ranges[i]:
                f.write(idx + '\n')
        #REMOVE THIS ON THE NEXT IMAGE BUILD
        py_scp_to_remote('', ip, 'get_chunks.sh', '~/get_chunks.sh')
        py_scp_to_remote('', ip, 'chunks-' + str(i), '~/chunks-' + str(i))
        py_scp_to_remote('', ip, glob.DATA_SET_PATH + '/' + glob.DATA_SET + '_meta_data_for_train_file', '~/train_file.' + str(i) + '.meta')
        #MOVE EVERYTHING USING SUDO
        py_ssh('', ip, 'mkdir ' + glob.REMOTE_PATH + '/data_loc' + ';' + 
                       'mv get_chunks.sh ' + glob.REMOTE_PATH + '/get_chunks.sh;' + 
                       'mv chunks* ' +  glob.REMOTE_PATH + '/data_loc;' + 
                       'mv tr* ' + glob.REMOTE_PATH + '/data_loc')
        #replace_s3cfg(ip)
        py_cmd_line('rm ' + 'chunks-' + str(i))
        py_ssh('', ip, 'source ' + glob.REMOTE_PATH + '/get_chunks.sh ' +
                       glob.DATA_PATH + ' ' + glob.REMOTE_CFG + ' ' +
                       data_path + ' ' + str(i) + ' ' + glob.REMOTE_PATH)
    return




def main():
    glob.set_globals()
    #USE THIS TO SPLIT CHUNKS LOCALLY
    chunk_data_locally('local', glob.DATA_SET_BUCKET, glob.DATA_SET, 1000)

    #USE THIS TO DELETE CHUNKS
    #delete_chunks('s3://' + glob.DATA_SET_BUCKET, glob.DATA_SET)

    #USE THIS TO DISTRIBUTE CHUNKS
    #distribute_chunks(['m3.2xlarge','m3.2xlarge','m3.2xlarge','m3.2xlarge'], glob.DATA_SET_BUCKET, glob.DATA_SET)

if __name__ == "__main__":
    main()

