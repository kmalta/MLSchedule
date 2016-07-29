import os
import sys
from subprocess import Popen, PIPE
from time import sleep, gmtime, strftime

#Dependency Files
import glob
from image_funcs import *
#import data_partition

glob.set_globals()

def time_str():
    return strftime("-%Y-%m-%d-%H-%M-%S", gmtime())

def read_instance_types():
    stdout = py_euca_describe_instance_types()
    stdout_array = stdout.split('\n')
    inst_types = []
    for line in stdout_array[1:]:
        line_array = line.split()
        if len(line_array) > 0:
            inst_types.append((line_array[1], int(line_array[2]), int(line_array[3]), int(line_array[4])))
    return inst_types

def create_hostfiles(ips, new_ips):
    print 'Creating Hostfiles...'
    f1 = open('./hostfile', 'w')
    for ip in ips:
        f1.write(ip + '\n')
    f1.close()
    f2 = open('./new_hostfile', 'w')
    for ip in new_ips:
        f2.write(ip + '\n')
    f2.close()
    f3 = open('./hostfile_petuum_format', 'w')

    for (j, ip) in enumerate(ips):
        if j != len(ips) - 1:
            f3.write(str(j) + ' ' + ip + ' 9999\n')
    if len(ips) > 0:
        f3.write(str(len(ips) - 1) + ' ' + ips[-1] + ' 9999')
    f3.close()

def replace_hostfiles(master_ip):
    print 'Replacing Hostfiles...'

    target_path = glob.REMOTE_PATH + '/bosen/machinefiles/hostfile_petuum_format'
    py_scp_to_remote('', master_ip, 'hostfile_petuum_format', target_path)

    f = open('hostfile', 'r')
    data = f.readlines()
    for i in range(len(data)):
        if data[i] == '':
            continue
        py_scp_to_remote('', data[i].strip(), 'hostfile_petuum_format', target_path)
    return

def passwordless_ssh(master_ip):
    print 'Moving Hostfile to Master...'
    py_scp_to_remote('', master_ip, 'hostfile', glob.REMOTE_PATH + '/hostfile')
    print 'Moving New Hostfile to Master...'
    py_scp_to_remote('', master_ip, 'new_hostfile', glob.REMOTE_PATH + '/new_hostfile')
    print 'Setting up Passwordless SSH...'
    py_ssh('', master_ip, 'source ' + glob.REMOTE_PATH + '/add_public_key_script.sh')
    print 'Finished Passwordless SSH'
    return

def add_ssh_key_to_master(master_ip):
    py_ssh('', master_ip, 'source ' + glob.REMOTE_PATH + '/create_ssh_keygen.sh')
    return


def push_launch_script_to_master(master_ip):
    py_scp_to_remote('', master_ip, 'launch.py', glob.REMOTE_PATH + '/bosen/app/mlr/script/launch.py')
    return


def clean_master_known_hosts(master_ip):
    py_ssh('', master_ip, 'sudo echo -n > /home/ubuntu/.ssh/known_hosts') 
    return

def terminate_instances(inst_ids, inst_ips):
    print "Terminating Instances...Hasta la vista baby!"
    f1 = open('/Users/Kevin/.ssh/known_hosts', 'r')
    known_host_lines = f1.readlines()
    f2 = open('temp_known_hosts', 'w')
    for line in known_host_lines:
        flag = 0
        for ip in inst_ips:
            if ip in line:
                flag = 1
        if flag == 0:
            f2.write(line)

    py_cmd_line('cp temp_known_hosts /Users/Kevin/.ssh/known_hosts')

    for inst_id in inst_ids:
        py_euca_terminate_instances(inst_id)

def launch_machine_learning_job(master_ip, argvs, remote_file_name):
    py_ssh('', master_ip, 'python ' + glob.REMOTE_PATH + '/bosen/app/mlr/script/launch.py ' + argvs + ' &> ' + file_name)
    return


def wait_for_file_to_write(master_ip, remote_file_name, local_file_path):
    while(1):
        sleep(20)
        local_file_path = local_file_dir + '/' + file_root
        proc = py_scp_to_local('', master_ip, remote_file_name, local_file_path)
        stdout = py_out_proc('cat ' + local_file_path)
        if 'MLR finished and shut down!' in stdout:
            break
    return


def run_ml_task(master_ip, inst_type, inst_count, epochs, cores, staleness, run, local_file_dir):

    inst_str = list(map(lambda x: str(x), inst_type))
    inst_str.append('machines')
    inst_str.append(str(inst_count))
    inst_str.append('run')
    inst_str.append(str(run))
    file_root = '_'.join(inst_str)
    remote_file_name = glob.REMOTE_PATH + '/' + file_root
    argvs = ' '.join([epochs, cores, staleness])

    launch_machine_learning_job(master_ip, argvs, remote_file_name)
    wait_for_file_to_write(master_ip, remote_file_name, local_file_dir + '/' + file_root)
    return


def run_experiment(master_inst_type, mach_array, data_set_name, runs):

    #CONSTANTS
    epochs = str(40)
    staleness = str(3)

    #Use this to only build the images
    force_uncache(master_inst_type)
    sys.exit("END SCRIPT")

    master_ip, master_id = launch_instance_with_metadata(master_inst_type, 'master')
    add_ssh_key_to_master(master_ip)
    push_launch_script_to_master(master_ip)

    local_file_dir = 'experiment_data/' + data_set_name + time_str()
    py_cmd_line('mkdir ' + local_file_dir)

    inst_types = read_instance_types()
    for inst_type in inst_types:
        py_cmd_line('mkdir ' + local_file_dir + '/' + inst_type[0])
        for i in mach_array:
            old_ips = check_instance_status('ips', 'running')
            launch_instances(i, inst_type[0], 'worker')
            ips = check_instance_status('ips', 'running')
            if len(ips) == len(old_ips):
                break
            ips.remove(master_ip)
            new_ips = filter(lambda x: x not in old_ips, ips)

            create_hostfiles(ips, new_ips)
            passwordless_ssh(master_ip)
            replace_hostfiles(master_ip)

            cores = str(inst_type[1])
            for j in range(runs):
                run_ml_task(master_ip, inst_type, len(ips), epochs, cores, staleness, j, local_file_dir)

        inst_ids = check_instance_status('ids', 'all')
        inst_ips = check_instance_status('ips', 'all')
        inst_ids.remove(master_id)
        inst_ips.remove(master_ip)
        terminate_instances(inst_ids, inst_ips)
        clean_master_known_hosts(master_ip)


#EXCLUDE PARTICULAR INSTANCES HERE
EXCLUDED_INSTANCE_TYPES = []


#RUN IT HERE!!
#Start with Clean Slate
inst_ids = check_instance_status('ids', 'all')
inst_ips = check_instance_status('ips', 'all')
terminate_instances(inst_ids, inst_ips)

run_experiment('m3.2xlarge', [1,1,2,4,8,16], 'mnist8m', 40)


