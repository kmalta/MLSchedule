import os
import sys
from subprocess import Popen, PIPE
from time import sleep, gmtime, strftime

#Dependency Files
import glob
from image_funcs import *
from swapfile_table import *

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

def get_machine_cores_from_names(machine_array):
    inst_types = read_instance_types()
    inst_names = map(lambda x: x[0], inst_types)
    machine_core_array = []
    for elem in machine_array:
        idx = inst_names.index(elem)
        machine_core_array.append(inst_types[idx][1])
    return machine_core_array


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

    get_path = glob.REMOTE_PATH + '/bosen/machinefiles/hostfile_petuum_format'
    py_scp_to_remote('', master_ip, 'hostfile_petuum_format', get_path)

    f = open('hostfile', 'r')
    data = f.readlines()
    for i in range(len(data)):
        if data[i] == '':
            continue
        py_scp_to_remote('', data[i].strip(), 'hostfile_petuum_format', get_path)
        py_scp_to_remote('', data[i].strip(), 'hostfile', glob.REMOTE_PATH + '/hostfile')
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
    py_ssh('', master_ip, 'python ' + glob.REMOTE_PATH + '/bosen/app/mlr/script/launch.py ' + argvs + ' &> ' + remote_file_name)
    return

def add_swapfile(ip, ram):
    swap = str(SWAP[RAM.index(int(ram))]*1024)
    py_scp_to_remote('', ip, 'make_swapfile.sh', glob.REMOTE_PATH)
    py_ssh('', ip, 'cd ' + glob.REMOTE_PATH + '; source make_swapfile.sh ' + swap)
    return


def wait_for_file_to_write(master_ip, remote_file_name, local_file_path):
    while(1):
        sleep(20)
        proc = py_scp_to_local('', master_ip, remote_file_name, local_file_path)
        stdout = py_out_proc('cat ' + local_file_path)
        if 'MLR finished and shut down!' in stdout:
            break
        if 'std::bad_alloc' in stdout:
            print '\n\n'
            print '##############################################################################'
            print '################################# BAD ALLOC ##################################'
            print '##############################################################################'
            print '\n\n'
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
    remote_pem = glob.REMOTE_PATH + '/' + glob.PEM_PATH
    use_weights = 'false'
    if run > 0:
        use_weights = 'true'

    argvs = ' '.join([epochs, cores, staleness, glob.DATA_PATH, use_weights, remote_pem])

    launch_machine_learning_job(master_ip, argvs, remote_file_name)
    wait_for_file_to_write(master_ip, remote_file_name, local_file_dir + '/' + inst_type[0] + '/' + file_root)
    return

#Creates Images if you so desire:
def main():
    glob.set_globals()
    inst_ids = check_instance_status('ids', 'all')
    inst_ips = check_instance_status('ips', 'all')
    terminate_instances(inst_ids, inst_ips)
    force_uncache('m3.2xlarge')

if __name__ == "__main__":
    main()

