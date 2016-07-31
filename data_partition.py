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



#NEEDS REFACTOR!

# def chunk_data_locally(file_bucket_path, chunk_bucket_path, filename, filesize):
#     os.system('find ./temp_data_chunks -maxdepth 1 -name "*" -print0 | xargs -0 rm')

#     # cmd1 = ['s3cmd -c s3cfg get ' + file_bucket_path + "/" + filename]
#     # proc1 = Popen(cmd1, shell=True, executable="/bin/bash")
#     # proc1.wait()

#     cmd2 = 'wc -l ' + filename
#     (stdout, stderr) = Popen(cmd2.split(), stdout=PIPE).communicate()

#     num_lines = int(stdout.split()[0])
    
#     cmd3 = 'ls -l ' + filename
#     (stdout, stderr) = Popen(cmd3.split(), stdout=PIPE).communicate()

#     file_byte_size = int(stdout.split()[4])
#     print file_byte_size
#     print num_lines
#     num_chunks = int(math.ceil(float(file_byte_size)/filesize))
#     print num_chunks

#     num_digits = int(math.ceil(math.log(num_chunks, 10)))

#     os.system(local_split + ' -n l/' + str(num_chunks) + ' -a ' + str(num_digits)
#               + ' -d ' + filename + ' temp_data_chunks/' + filename + '-chunk-')

#     cmd4 = 'ls -l ' + 'temp_data_chunks/' + filename + '-chunk-' + num_digits*"0"
#     (stdout, stderr) = Popen(cmd4.split(), stdout=PIPE).communicate()
#     chunk_byte_size = int(stdout.split()[4])


#     metadata_filepath = 'temp_data_chunks/' + filename + '-chunk-' + 'metadata'

#     f = open(metadata_filepath, 'w')
#     f.write('file_byte_size' + '\t' + 'num_chunks' + '\t'
#             + 'num_digits' + '\t' + 'chunk_byte_size' + '\n')
#     f.write(str(file_byte_size) + '\t' + str(num_chunks) + '\t'
#             + str(num_digits) + '\t' + str(chunk_byte_size) + '\n')

#     os.system('s3cmd -c s3cfg put ' + metadata_filepath + ' ' + chunk_bucket_path)

#     for i in range(num_chunks):
#         num_zeros = num_digits - len(str(i))
#         filepath_to_upload = 'temp_data_chunks/' + filename + '-chunk-' + num_zeros*"0" + str(i)
#         os.system('s3cmd -c s3cfg put ' + filepath_to_upload + ' ' + chunk_bucket_path)


# def delete_chunks(chunk_bucket_path, filename):
#     os.system('s3cmd -c s3cfg del ' + chunk_bucket_path + '/' + filename + '-chunk-*')

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


def distribute_chunks(machine_array, data_chunk_bucket_path, data_name):

    py_cmd_line('rm *-chunk-metadata')
    py_s3cmd_get(data_chunk_bucket_path + '/*-chunk-metadata')
    f = open(data_name + '-chunk-metadata', 'r')
    data = f.readlines()[1].split()
    num_chunks = int(data[1])
    num_digits = int(data[2])

    machine_cores_array = get_machine_cores_from_names(machine_array)
    chunk_partitions = doll_out_chunks(num_chunks, machine_cores_array)

    chunk_ranges = assign_data_chunks(chunk_partitions, num_digits)

    f2 = open('hostfile', 'r')
    host_ips = f2.readlines()

    for i in range(len(chunk_ranges)):
        ip = host_ips[i]

        for chunk_idx in chunk_ranges[i]:
            s3cmd_str = ['s3cmd -c ' + glob.REMOTE_PATH + '/data_loc '
                         + ' get s3://' + data_chunk_bucket_path + '/'
                         + '-chunk-' + chunk_idx]
            py_ssh('', ip, s3cmd_str[0])


        py_ssh('', ip, 'cat ' + glob.REMOTE_PATH + '/data_loc/*.txt >> train_file.' + i)
        








#Creates Images if you so desire:
def main():
    distribute_chunks(['m3.2xlarge','m3.2xlarge','m3.2xlarge','m3.2xlarge'], 'mnist-data', 'mnist8m')

if __name__ == "__main__":
    main()



#print compute_num_chunks_to_distribute([32,16,8,8,8,2])











#delete_chunks('s3://mnist-data', 'mnist8m')

#chunk_data_locally('s3://mnist-data','s3://mnist-data', 'mnist8m', 100000000)