import os
import sys
from subprocess import Popen, PIPE
from time import sleep, gmtime, strftime

#Dependency Files
import glob
from image_funcs import *

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

def read_host_ips():
    f = open('hostfile', 'r')
    host_ips = f.readlines()
    return filter(lambda y: y != '', map(lambda x: x.strip(), host_ips))

def get_machine_attributes_from_names(machine_array, attr):
    inst_types = read_instance_types()
    attr_idx = 1
    if attr == 'cores':
        attr_idx = 1
    if attr == 'ram':
        attr_idx = 2
    inst_names = map(lambda x: x[0], inst_types)
    machine_attr_array = []
    for elem in machine_array:
        idx = inst_names.index(elem)
        machine_attr_array.append(inst_types[idx][attr_idx])
    return machine_attr_array


def create_hostfiles(ips, new_ips):
    print 'Creating Hostfiles...'
    f1 = open('temp_files/hostfile', 'w')
    for ip in ips:
        f1.write(ip + '\n')
    f1.close()
    f2 = open('temp_files/new_hostfile', 'w')
    for ip in new_ips:
        f2.write(ip + '\n')
    f2.close()

def replace_hostfiles(master_ip):
    print 'Replacing Hostfiles...'

    f = open('temp_files/hostfile', 'r')
    data = f.readlines()
    for i in range(len(data)):
        if data[i] == '':
            continue
        py_scp_to_remote('', data[i].strip(), 'temp_files/hostfile', glob.REMOTE_PATH + '/hostfile')
    return



def passwordless_ssh(master_ip, master_port, up, slave_ips):
    print 'Moving Hostfile to Master...'
    py_scp_to_remote('', master_ip, 'temp_files/hostfile', glob.REMOTE_PATH + '/hostfile')
    print 'Moving New Hostfile to Master...'
    py_scp_to_remote('', master_ip, 'temp_files/new_hostfile', glob.REMOTE_PATH + '/new_hostfile')
    print 'Setting up Passwordless SSH...'

    print 'Finished Passwordless SSH'
    return


def add_ssh_key_to_master(master_ip):
    py_ssh('', master_ip, 'source ' + glob.REMOTE_PATH + '/create_ssh_keygen.sh')
    return


def push_launch_script_to_master(master_ip):
    py_scp_to_remote('', master_ip, 'temp_files/launch.py', glob.REMOTE_PATH + '/bosen/app/mlr/script/launch.py')
    return


def clean_master_known_hosts(master_ip):
    py_ssh('', master_ip, 'sudo echo -n > /home/ubuntu/.ssh/known_hosts') 
    return

def terminate_instances(inst_ids, inst_ips):
    print "Terminating Instances...Hasta la vista baby!"

    for inst_id in inst_ids:
        py_euca_terminate_instances(inst_id)

    #Make sure to allow the instances to become available before a relaunch
    print "Sleeping after termination"
    sleep(60)
    print "Woke up after termination"

def launch_machine_learning_job(master_ip, argvs, remote_file_name):
    py_ssh('', master_ip, 'python ' + glob.REMOTE_PATH + '/bosen/app/mlr/script/launch.py ' + argvs + ' &> ' + remote_file_name)
    return

def check_for_s3_403():
    f = open('temp_files/hostfile', 'r')
    ips = f.readlines()
    for ip in ips:
        ip = ip.strip()
        if ip != '':
            py_scp_to_local('', ip, '~/s3error', 'temp_files/s3error_check')
            f = open('temp_files/s3error_check', 'r')
            if '(Forbidden)' in f.read():
                print "We found an S3 ERROR: 403 (Forbidden).  Don't be alarmed! It was caught."
                return 1
    return 0




#Creates Images if you so desire:
def main():
    glob.set_globals()
    inst_ids = check_instance_status('ids', 'all')
    inst_ips = check_instance_status('ips', 'all')
    terminate_instances(inst_ids, inst_ips)
    create_image('m3.2xlarge')


if __name__ == "__main__":
    main()

