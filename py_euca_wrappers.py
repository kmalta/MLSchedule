import python_cmd_line_wrappers

def py_euca_describe_instance_types():
    return py_out_proc('euca-describe-instance-types ' + REGION_STR)

def py_euca_describe_instances():
    return py_out_proc('euca-describe-instances ' + REGION_STR)

def py_euca_run_instances(image, num_inst, inst_type):
    cmd = 'euca-run-instances ' + image + ' --instance-count ', str(num_inst) + ' '
          KEY_STR + ' ' + GROUP_STR + ' --instance-type ' + inst_type + ' ' + REGION_STR
    (stdout, stderr) = Popen(cmd.split(), stdout=PIPE).communicate()
    return stdout

def py_euca_deregister(image_id):
    py_cmd_line('euca-deregister ' + image_id + REGION_STR)
    return

def py_euca_delete_bundle(inst_role):
    py_cmd_line('euca-delete-bundle ' + BUCKET_STR + ' --prefix ' + inst_role + PREFIX_SUFFIX + ' ' + REGION_STR)
    return

def py_euca_delete_bundle(inst_role):
    py_cmd_line('euca-delete-bundle ' + BUCKET_STR +  ' --prefix ' + inst_role + PREFIX_SUFFIX + ' ' + REGION_STR)
    return

def py_euca_describe_bundle_tasks():
    return py_out_proc('euca-describe-bundle-tasks ' + REGION_STR)

def py_euca_register(inst_role):
    cmd = 'euca-register -n ' + inst_role + PREFIX_SUFFIX + ' ' + REGION_STR + ' ' +
          VIRT_TYPE_STR + ' ' + BUCKET + '/' + inst_role + PREFIX_SUFFIX + '.manifest.xml'
    return py_out_proc(cmd)

        cmd = ['euca-terminate-instances', inst_id, '--region', 'REGION']

def py_euca_terminate_instances(inst_id):
    py_out_proc('euca-terminate-instances ' + inst_id + ' ' + REGION_STR)
    return
