#Dependency Files
import glob
from launch_and_wait import *


def create_image(inst_type):
    print 'Creating image...'
    py_s3cmd_mb(glob.BUCKET)
    _ip, _id = launch_instance_with_metadata(inst_type)
    #_ip = '128.111.179.252'
    #_id = 'i-3218ba99'
    #image_id = '0'
    setup_instance(_ip)
    image_id = read_image_id()
    if image_id != 0:
        py_euca_deregister(image_id)
        py_euca_deregister(image_id)
        py_euca_delete_bundle()
        print 'Deregistered and sleeping for 2 minutes...'
        sleep(120)

    py_euca_bundle_instance(_id)

    while(1):
        sleep(10)
        stdout = py_euca_describe_bundle_tasks()

        stdout_array = stdout.split('\n')
        flag = 0
        for line in stdout_array:
            inst_info = line.split()
            if len(inst_info) > 0 and (inst_info[0] == 'BUNDLE') and (inst_info[7] != 'complete') and (inst_info[7] != 'failed'):# and (inst_info[7] != 'storing'):
                print 'Need to wait...the bundles are not bundled yet, hold your bundles...\t\t', inst_info[8], '% complete'
                flag = 1
        if flag == 1:
            continue
        break

    stdout = py_euca_register()
    try:
        image_id = stdout.split('\n')[0].split()[1]
    except:
        print "ERROR:", stdout

    py_cmd_line('echo ' + image_id + ' > ' + glob.CLOUD + '/' + glob.CLOUD + '_image_id')
    return
