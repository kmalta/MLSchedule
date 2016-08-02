#Dependency Files
import glob
from launch_and_wait import *

MASTER_CHECK_SUM_ARRAY = [
                          'add_public_key_script.sh',
                          'build_dependencies.sh',
                          'build_image_script.sh',
                         ]

WORKER_CHECK_SUM_ARRAY = [
                          'build_dependencies.sh',
                          'build_image_script.sh',
                         ]

CSA_DICT = {
            'master_csa': MASTER_CHECK_SUM_ARRAY,
            'worker_csa': WORKER_CHECK_SUM_ARRAY,
           }

def check_or_create_image(inst_type):
    if glob.REPLACE_WHICH == 'both':
        if check_sum('master') == 0:
            create_image(inst_type, 'master')
        if check_sum('worker') == 0:
            create_image(inst_type, 'worker')
    if glob.REPLACE_WHICH == 'master':
        if check_sum('master') == 0:
            create_image(inst_type, 'master')
    if glob.REPLACE_WHICH == 'worker':
        if check_sum('worker') == 0:
            create_image(inst_type, 'worker')


def force_uncache(master_inst_type):
    if glob.LAUNCH_FROM == 'scratch':
        if glob.REPLACE_WHICH == 'both':
            for inst_role in ['master', 'worker']:
                file_name = inst_role + '_check_sum_cache'
                if os.path.isfile(file_name):
                    py_cmd_line('rm ' + file_name + ';touch ' + file_name)
        else:
            file_name = glob.REPLACE_WHICH + '_check_sum_cache'
            if os.path.isfile(file_name):
                py_cmd_line('rm ' + file_name + ';touch ' + file_name)
        check_or_create_image(master_inst_type)
    return


def check_sum(inst_role):
    file_name = inst_role + '_check_sum'
    if os.path.isfile(file_name):
        py_cmd_line('rm ' + file_name)

    script_array = CSA_DICT[inst_role + '_csa']

    for script in script_array:
        py_cmd_line('cat ' + script + ' | md5 >> ' + file_name + ' 2>&1')
    f1 = open(file_name, 'r')
    f2 = open(file_name + '_cache', 'r')
    cache = f2.read()
    if cache in f1.read() and cache != '' :
        return 1
    else:
        py_cmd_line('cp ' + file_name + ' ' + file_name + '_cache')
        return 0

def create_image(inst_type, inst_role):
    print 'Creating', inst_role, 'image...'
    py_s3cmd_mb(glob.BUCKET)
    _ip, _id = launch_instance_with_metadata(inst_type, 'neither')
    setup_instance(_ip, inst_role)
    image_id = read_image_id(inst_role)
    if image_id != 0:
        py_euca_deregister(image_id)
        py_euca_deregister(image_id)
        py_euca_delete_bundle(inst_role)
        print 'Deregistered and sleeping for 2 minutes...'
        sleep(120)

    py_euca_bundle_instance(inst_role, _id)

    while(1):
        sleep(10)
        stdout = py_euca_describe_bundle_tasks()

        stdout_array = stdout.split('\n')
        flag = 0
        for line in stdout_array:
            inst_info = line.split()
            if len(inst_info) > 0 and (inst_info[0] == 'BUNDLE') and (inst_info[7] != 'complete') and (inst_info[7] != 'failed'):
                print 'Need to wait...the bundles are not bundled yet, hold your bundles...\t\t', inst_info[8], '% complete'
                flag = 1
        if flag == 1:
            continue
        break

    stdout = py_euca_register(inst_role)
    image_id = stdout.split('\n')[0].split()[1]
    py_cmd_line('echo ' + image_id + ' > ' + glob.CLOUD + '/' + glob.CLOUD + '_' + inst_role + '_image_id')
    return
