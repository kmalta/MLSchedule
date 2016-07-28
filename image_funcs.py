#Dependency Files
from py_euca_wrappers import *

MASTER_CHECK_SUM_ARRAY = [
                          'add_public_key_script.sh',
                          'build_master_dependencies.sh',
                          'master_setup_script.sh',
                          'replace_hostfiles.sh',
                          'build_image_script.sh',
                         ]

WORKER_CHECK_SUM_ARRAY = [
                          'build_worker_dependencies.sh',
                          'worker_setup_script.sh',
                          'replace_hostfiles.sh',
                          'build_image_script.sh',
                         ]

CSA_DICT = {
            'master_csa': MASTER_CHECK_SUM_ARRAY,
            'worker_csa': WORKER_CHECK_SUM_ARRAY,
           }



def check_or_create_image(inst_type):
    if REPLACE_WHICH == 'both':
        if check_sum('master') == 0:
            create_image(inst_type, 'master')
        if check_sum('worker') == 0:
            create_worker_image(inst_type, 'worker')
    if REPLACE_WHICH == 'master':
        if check_sum('master') == 0:
            create_image(inst_type, 'master')
    if REPLACE_WHICH == 'worker':
        if check_sum('worker') == 0:
            create_image(inst_type, 'worker')


def force_uncache(master_inst_type):
    if LAUNCH_FROM == 'scratch':
        if REPLACE_WHICH == 'both':
            for inst_role in ['master', 'worker']:
                py_cmd_line('rm ' + inst_role + '_check_sum_cache;touch ' + inst_role + '_check_sum_cache')
        else:
            py_cmd_line('rm ' + REPLACE_WHICH + '_check_sum_cache;touch ' + REPLACE_WHICH + '_check_sum_cache')
        check_or_create_image(master_inst_type, REPLACE_WHICH)
    return


def read_image_id(inst_role):
    if inst_role not in ['master', 'worker']:
        return 0
    f = open(inst_role + '_image_id', 'r')
    image_id = f.read()
    if 'emi' in image_id:
        return image_id.strip()
    else:
        return 0


def check_sum(inst_role):

    py_cmd_line('rm ' + inst_role + '_check_sum')

    script_array = CSA_DICT[inst_role + '_csa']

    for script in script_array:
        py_cmd_line('cat ' + script + ' | md5 >> master_check_sum 2>&1')
    f1 = open(inst_role + '_check_sum', 'r')
    f2 = open(inst_role + '_check_sum_cache', 'r')
    if f2.read() in f1.read():
        return 1
    else:
        py_cmd_line('cp ' + inst_role + '_check_sum ' + inst_role + '_check_sum_cache')
        return 0


def create_image(inst_type, inst_role):
    _ip, _id = launch_instance_with_metadata(inst_type, 'neither')
    setup_instance(_ip, inst_role)
    image_id = read_image_id(inst_role)
    if image_id != 0:
        py_euca_deregister(image_id)
        py_euca_delete_bundle(inst_role)
    py_euca_delete_bundle(inst_role)

    while(1):
        sleep(10)
        stdout = py_euca_describe_bundle_tasks()

        stdout_array = stdout.split('\n')
        flag = 0
        for line in stdout_array:
            inst_info = line.split()
            if len(inst_info) > 0 and (inst_info[0] == 'BUNDLE') and (inst_info[7] != 'complete'):
                print 'Need to wait...the bundles are not bundled yet, hold your bundles...\t\t', inst_info[8], '% complete'
                flag = 1
        if flag == 1:
            continue
        break

    stdout = py_euca_register(inst_role)
    image_id = stdout.split('\n')[0].split()[1]
    py_cmd_line('echo ' + image_id + ' > ' + inst_role + '_image_id')
    return

