#Dependency Files
import glob
from deploy_cloud import *


def deploy_schedule(master_inst_type, mach_array, data_set_name, data_chunk_bucket_path, runs):

    #CONSTANTS
    epochs = str(40)
    staleness = str(3)

    #Use this to only build the images
    force_uncache(master_inst_type)

    #mach_config = init_config()
    mach_config = ['m3.2xlarge', 'm3.2xlarge', 'm3.2xlarge', 'm3.2xlarge']

    master_ip, master_id = launch_instance_with_metadata(master_inst_type, 'master')
    add_ssh_key_to_master(master_ip)

    for inst in mach_config:
        launch_instances(1, inst, 'worker')


    core_config = get_cores_from_machs(mach_config)

    chunk_config = compute_num_chunks_to_distribute(core_config, data_chunk_bucket_path)
    distribute_chunks(num_chunk_array, data_chunk_bucket_path)



    sys.exit("END SCRIPT")

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
