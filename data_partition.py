import numpy as np
import os
import sys
from subprocess import Popen, PIPE
from time import sleep
import math

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

def looped_assign(num_chunks, percentage_cores_array):
    curr_chunk_count = num_chunks
    chunk_assignment_array = [0 for i in range(len(percentage_cores_array))]
    while(curr_chunk_count > 0):
        print chunk_assignment_array
        chunks_to_assign = [int(math.floor(curr_chunk_count*i)) for i in percentage_cores_array]
        chunk_assignment_array = [x + y for x, y in zip(chunk_assignment_array, chunks_to_assign)]
        used_chunks = sum(chunks_to_assign)
        curr_chunk_count -= used_chunks

def compute_num_chunks_to_distribute(machine_cores_array):
    # py_cmd_line('rm *-chunk-metadata')
    # py_s3cmd_get(data_chunk_bucket_path + '/*-chunk-metadata')
    # f = open('*-chunk-metadata', 'r')
    # data = f.readlines()[1].split()
    num_chunks = 517#int(data[1])

    total_cores = sum(machine_cores_array)

    return looped_assign(num_chunks, [float(i)/total_cores for i in machine_cores_array])

def distribute_chunks(num_chunk_array, data_chunk_bucket_path):

    py_s3cmd_get(data_chunk_bucket_path + '/*-chunk-metadata')

    os.system('mkdir temp_metadata_file')
    f = open('temp_metadata_file/*-chunk-metadata', 'r')

    data = f.readlines()[1].split()

compute_num_chunks_to_distribute([4,4,4,4])











#delete_chunks('s3://mnist-data', 'mnist8m')

chunk_data_locally('s3://mnist-data','s3://mnist-data', 'mnist8m', 100000000)