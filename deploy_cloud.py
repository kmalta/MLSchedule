import os
import sys
from subprocess import Popen, PIPE
from time import sleep, gmtime, strftime

#Dependency Files
import glob
from image_funcs import *
#import data_partition

glob.set_globals()
print glob.CLOUD

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

def check_instance_status(info, inst_status):
    stdout = py_euca_describe_instances()
    ret_vals = []
    stdout_array = stdout.split('\n')

    idx = 0
    if info == 'ids':
        idx = 1
    elif info == 'ips':
        idx = 12
    for line in stdout_array:
        inst_info = line.split()
        if len(inst_info) > 0 and (inst_info[0] == 'INSTANCE') :
            check_bool = True
            if inst_status == 'running':
                check_bool = inst_info[5] == 'running'
            elif inst_status == 'all':
                check_bool = inst_info[5] != 'terminated'

            if check_bool:
                ret_vals.append(inst_info[idx])
    return ret_vals

#WAITS


def wait_for_nodes_to_launch():
    while(1):
        sleep(10)
        stdout = py_euca_describe_instances()
        ret_vals = []
        stdout_array = stdout.split('\n')
        flag = 0
        for line in stdout_array:
            inst_info = line.split()
            if len(inst_info) > 0 and (inst_info[0] == 'INSTANCE') and (inst_info[5] == 'pending'):
                print 'Need to wait...the nodes are not running yet'
                flag = 1
        if flag == 1:
            continue
        break
    return


def wait_ssh(ips):
    for ip in ips:
        count = 0
        while(1):
            proc = py_ssh('-o connecttimeout=3', ip,'true 2>/dev/null')
            if proc.returncode == 0:
                break
            count += 1
            if count % 100 == 1:
                print 'Waiting on node', ip
    return



#LAUNCHES


def launch_instances(num_insts, inst_type, inst_role):
    print 'Starting to launch an instance from an image'

    image = read_image_id(inst_role)

    if image == 0:
        image = glob.BASE_IMAGE

    stdout = py_euca_run_instances(image, num_insts, inst_type)
    wait_for_nodes_to_launch()
    ips = check_instance_status('ips', 'running')
    wait_ssh(ips)
    return stdout


def launch_instance_with_metadata(inst_type, inst_role):
    stdout = launch_instances(1, inst_type, inst_role)
    out_array = stdout.split('\n')[1].split()
    _ip = out_array[12]
    _id = out_array[1]
    return _ip, _id


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



def setup_instance(ip, inst_role):
    if inst_role == 'master':
        cmd = ('tar -cf scripts.tar.gz ' + glob.PEM_PATH +
              'add_public_key_script.sh build_image_script.sh create_ssh_keygen.sh')
        py_cmd_line(cmd[0])
    elif inst_role == 'worker':
        cmd = ('tar -cf scripts.tar.gz ' + glob.PEM_PATH + 
              'build_image_script.sh')
        py_cmd_line(cmd[0])
    else:
        sys.exit('ERROR: In setup_instance function')

    target_path = '/'.join(glob.REMOTE_PATH.split('/')[:-1])
    py_scp_to_remote('', ip, 'scripts.tar.gz', target_path)
    py_scp_to_remote('', ip, 'build_dependencies.sh', target_path)
    py_ssh('', ip, 'source ' + target_path + '/build_dependencies.sh ' + target_path)
    return


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
    inst_string.append('run')
    inst_string.append(str(run))
    file_root = '_'.join(inst_str)
    remote_file_name = glob.REMOTE_PATH + '/' + file_root
    argvs = ' '.join(epochs, cores, staleness)

    launch_machine_learning_job(master_ip, argvs, remote_file_name)
    wait_for_file_to_write(master_ip, remote_file_name, local_file_dir + '/' + file_root)
    return


def run_experiment(master_inst_type, mach_array, data_set_name, runs):

    #CONSTANTS
    epochs = str(40)
    staleness = str(3)

    #Use this to only build the images
    force_uncache(master_inst_type)
    #sys.exit("END SCRIPT")

    master_ip, master_id = launch_instance_with_metadata(master_inst_type, 'master')
    add_ssh_key_to_master(master_ip)
    push_launch_script_to_master(master_ip)

    local_file_dir = 'experiment_data/' + data_set_name + time_str()
    py_cmd('mkdir ' + local_file_dir)

    inst_types = read_instance_types()
    for inst_type in inst_types:
        py_cmd('mkdir ' + local_file_dir + '/' + inst_type[0])
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


