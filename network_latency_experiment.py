from time import sleep, gmtime, strftime, time

#Dependency Files
import glob
from deploy_cloud import *
from data_partition import *

#DATA CAN BE FOUND AT:
#http://www.csie.ntu.edu.tw/~cjlin/libsvmtools/datasets/multiclass.html

def write_times(ssut, esut, sfut, efut, spdt, epdt, file_path, num_machs):
    f = open(file_path + '_times_' + str(num_machs), 'w')
    f.write('spin up time' + '\t' + 'file upload time' + '\t'
            + 'partition data time' + '\n')
    f.write(str(esut - ssut) + '\t' + str(efut - sfut) + '\t'
            + str(epdt - spdt) + '\n')
    

def run_experiment(master_inst_type, mach_array, data_set_name, data_bucket_name, runs):

    #CONSTANTS
    epochs = str(40)
    staleness = str(3)

    #Use this to only build the images

    master_ip, master_id = launch_instance_with_metadata(master_inst_type, 'master')
    add_ssh_key_to_master(master_ip)
    push_launch_script_to_master(master_ip)

    local_file_dir = glob.DATA_SET_PATH + '/' + data_set_name + time_str()
    py_cmd_line('mkdir ' + local_file_dir)

    #INSTANCE TYPES AND FILTERS
    inst_types = read_instance_types()
    inst_types_filter = filter(lambda x: int(x[3]) > 13 , inst_types)
    #inst_types_filter = filter(lambda x: int(x[2]) > 10000, inst_types_filter)

    for inst_type in inst_types_filter:
        py_cmd_line('mkdir ' + local_file_dir + '/' + inst_type[0])
        for i in mach_array:
            old_ips = []

            #SPIN UP INSTANCES
            ssut = time()
            launch_instances(i, inst_type[0], 'worker')
            esut = time()

            ips = check_instance_status('ips', 'running')
            ips.remove(master_ip)
            if len(ips) == 0:
                break
            new_ips = filter(lambda x: x not in old_ips, ips)

            #WRITE FILES
            sfut = time()
            create_hostfiles(ips, new_ips)
            passwordless_ssh(master_ip)
            replace_hostfiles(master_ip)
            for ip in new_ips:
                add_swapfile(ip, inst_type[2])
            efut = time()

            #PARTITION DATA
            spdt = time()
            distribute_chunks([inst_type[0] for i in range(len(ips))], data_bucket_name, data_set_name)
            epdt = time()

            #RUN MACHINE LEARNING JOB
            cores = str(inst_type[1])
            for j in range(runs):
                run_ml_task(master_ip, inst_type, len(ips), epochs, cores, staleness, j, local_file_dir)
            time_loc = local_file_dir + '/' + inst_type[0] + '/' + inst_type[0]
            write_times(ssut, esut, sfut, efut, spdt, epdt, time_loc, i)

            #CREATE CLEAN SLATE
            inst_ids = check_instance_status('ids', 'all')
            inst_ips = check_instance_status('ips', 'all')
            inst_ids.remove(master_id)
            inst_ips.remove(master_ip)
            terminate_instances(inst_ids, inst_ips)
            clean_master_known_hosts(master_ip)


#EXCLUDE PARTICULAR INSTANCES HERE
EXCLUDED_INSTANCE_TYPES = ['m1.small', 't1.micro', 'm1.medium', 'c1.medium']

def main():
    glob.set_globals()
    inst_ids = check_instance_status('ids', 'all')
    inst_ips = check_instance_status('ips', 'all')
    terminate_instances(inst_ids, inst_ips)

    run_experiment('m3.2xlarge', [1,2,4,8,16], glob.DATA_SET, glob.DATA_SET_BUCKET, 1)

if __name__ == "__main__":
    main()
