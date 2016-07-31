#Dependency Files
import glob
from py_command_line_wrappers import *


def py_euca_describe_instance_types():
    return py_out_proc('euca-describe-instance-types ' + glob.REGION_STR)

def py_euca_describe_instances():
    cmd = 'euca-describe-instances ' + glob.REGION_STR
    return py_out_proc('euca-describe-instances ' + glob.REGION_STR)

def py_euca_run_instances(image, num_inst, inst_type):
    cmd = ['euca-run-instances ' + image + ' --instance-count ' + str(num_inst) + ' ' +
          glob.KEY_STR + ' ' + glob.SECURITY_GROUP_STR + ' --instance-type ' + inst_type + ' ' + glob.REGION_STR]
    return py_out_proc(cmd[0])

def py_euca_deregister(image_id):
    py_cmd_line('euca-deregister ' + image_id + ' ' +  glob.REGION_STR)
    return

def py_euca_delete_bundle(inst_role):
    py_cmd_line('euca-delete-bundle ' + glob.BUCKET_STR + ' --prefix ' + inst_role + glob.PREFIX_SUFFIX + ' --clear ' + glob.REGION_STR)
    return

def py_euca_bundle_instance(inst_role, _id):
    py_cmd_line('euca-bundle-instance ' + glob.BUCKET_STR + ' --prefix ' + inst_role + glob.PREFIX_SUFFIX + ' ' + glob.REGION_STR + ' ' + _id)
    return

def py_euca_describe_bundle_tasks():
    return py_out_proc('euca-describe-bundle-tasks ' + glob.REGION_STR)

def py_euca_register(inst_role):
    cmd = ['euca-register -n ' + inst_role + glob.PREFIX_SUFFIX + ' ' + glob.REGION_STR + ' ' +
          glob.VIRT_TYPE_STR + ' ' + glob.BUCKET + '/' + inst_role + glob.PREFIX_SUFFIX + '.manifest.xml']
    return py_out_proc(cmd[0])

def py_euca_terminate_instances(inst_id):
    py_out_proc('euca-terminate-instances ' + inst_id + ' ' + glob.REGION_STR)
    return


def py_s3cmd_mb(bucket_name):
    py_wait_proc('s3cmd -c ' + glob.S3CMD_CFG_PATH + ' mb s3://' + bucket_name)
    return

def py_s3cmd_get(file_path):
    py_wait_proc('s3cmd -c ' + glob.S3CMD_CFG_PATH + ' get  s3://' + file_path)
    return
