from time import sleep, gmtime, strftime, time

#Dependency Files
import glob
from deploy_cloud import *
from data_partition import *

#DATA CAN BE FOUND AT:
#http://www.csie.ntu.edu.tw/~cjlin/libsvmtools/datasets/multiclass.html

def write_times(total_spin_up_time, total_file_creation_overhead_time, total_data_fetch_time, total_machine_learning_time, out_file_name):
    f = open(out_file_name, 'a')
    f.write('OVERHEAD STATS\n\n')
    f.write('spin up time: ' + str(total_spin_up_time) + '\n')
    f.write('file creation overhead time: ' + str(total_file_creation_overhead_time) + '\n')
    f.write('total data fetch time: ' + str(total_data_fetch_time) + '\n')
    f.write('total machine learning run time: ' + str(total_machine_learning_time) + '\n')
    f.close()



def run_experiment(master_inst_type, mach_array, data_set_name, data_bucket_name, runs, run_dependency, epochs, staleness_array, projected, chunks_to_use, init_time_str, ignore):

    #Use this to only build the images

    master_ip, master_id = launch_instance_with_metadata(master_inst_type, 'master')
    add_ssh_key_to_master(master_ip)
    push_launch_script_to_master(master_ip)
    py_cmd_line('echo ' + master_ip + ' > host_master')

    if init_time_str == '':
        init_time_str = time_str()
    local_file_dir = glob.DATA_SET_PATH + '/' + data_set_name + init_time_str
    py_cmd_line('mkdir ' + local_file_dir)

    #INSTANCE TYPES AND FILTERS
    inst_types = read_instance_types()
    inst_types_filter = filter(lambda x: int(x[3]) > 13 , inst_types)
    inst_types_filter = filter(lambda x: 'cg1.4xlarge' in x[0], inst_types_filter)
    #inst_types_filter = filter(lambda x: 'm' in x[0], inst_types_filter)
    if len(ignore) != 0:
        for i in range(len(ignore)):
            inst_types_filter = filter(lambda x: ignore[i] not in x[0], inst_types_filter)
    #inst_types_filter = filter(lambda x: 'm3.x' in x[0], inst_types_filter)

    for inst_type in inst_types_filter:
        exp_dir = local_file_dir + '/' + inst_type[0]
        py_cmd_line('mkdir ' + exp_dir)
        for i in mach_array:
            old_ips = []

            #Check if config will work
            mach_name_array = [inst_type[0] for k in range(i)]
            num_chunks, num_digits, remainder, chunk_parts = distribute_chunks(mach_name_array, data_bucket_name, data_set_name, projected, chunks_to_use)
            if remainder != 0:
                print 'WE ARE SKIPPING THIS CONFIGURATION, THERE ISN\'T ENOUGH SPACE OR MEMORY'
                continue

            #SPIN UP INSTANCES
            spin_up_start_time = time()
            launch_instances(i, inst_type[0], 'worker')
            spin_up_end_time = time()
            total_spin_up_time = spin_up_end_time - spin_up_start_time

            ips = check_instance_status('ips', 'running')
            ips.remove(master_ip)
            if len(ips) == 0:
                break
            new_ips = filter(lambda x: x not in old_ips, ips)

            #WRITE FILES
            file_creation_overhead_start_time = time()

            create_hostfiles(ips, new_ips)
            passwordless_ssh(master_ip)
            replace_hostfiles(master_ip)
            # py_scp_to_remote('', master_ip, '~/bosen/app/mlr/src/mlr_engine.cpp', 
            #                  glob.REMOTE_PATH + '/bosen/app/mlr/src/mlr_engine.cpp')
            # py_ssh('', master_ip, 'cd ' + glob.REMOTE_PATH + '/bosen/app/mlr; sudo make')
            for ip in ips:
                add_swapfile(ip, inst_type[2])
                # py_scp_to_remote('', ip, '~/bosen/app/mlr/src/mlr_engine.cpp', 
                #                  glob.REMOTE_PATH + '/bosen/app/mlr/src/mlr_engine.cpp')
                # py_ssh('', ip, 'cd ' + glob.REMOTE_PATH + '/bosen/app/mlr; sudo make')


            file_creation_overhead_end_time = time()
            total_file_creation_overhead_time = file_creation_overhead_end_time - file_creation_overhead_start_time

            create_hostfiles_petuum_format(chunk_parts, ips)
            replace_hostfiles_petuum_format(master_ip)

            #RUN MACHINE LEARNING JOB

            cores = str(inst_type[1])


            out_file_name = ''
            data_fetch_start_time = 0
            data_fetch_end_time = 0
            total_data_fetch_start_time = 0
            machine_learning_start_time = 0
            machine_learning_end_time = 0
            total_machine_learning_time = 0


            #Fetch Data
            flag = 1
            while flag ==1:
                data_fetch_start_time = time()
                data_fetch(chunk_parts, num_digits, data_bucket_name, data_set_name)
                flag = check_for_s3_403()
                data_fetch_end_time = time()
                total_data_fetch_time = data_fetch_end_time - data_fetch_start_time

            for staleness in staleness_array:
                for j in range(runs):


                    machine_learning_start_time = time()
                    remote_file_name, out_file_name = run_ml_task(master_ip, ips[0], inst_type, len(ips), cores, str(staleness), j, epochs, exp_dir, run_dependency)
                    wait_for_file_to_write(master_ip, ips[0], remote_file_name, out_file_name)
                    machine_learning_end_time = time()

                    total_machine_learning_time = machine_learning_end_time - machine_learning_start_time

                    kill_ml_task(master_ip)
                    write_times(total_spin_up_time, total_file_creation_overhead_time, 
                               total_data_fetch_time, total_machine_learning_time, out_file_name)



            #CREATE CLEAN SLATE
            inst_ids = check_instance_status('ids', 'all')
            inst_ips = check_instance_status('ips', 'all')
            inst_ids.remove(master_id)
            inst_ips.remove(master_ip)
            terminate_instances(inst_ids, inst_ips)
            clean_master_known_hosts(master_ip)

    inst_ids = check_instance_status('ids', 'all')
    inst_ips = check_instance_status('ips', 'all')
    terminate_instances(inst_ids, inst_ips)




def main():
    glob.set_globals()
    inst_ids = check_instance_status('ids', 'all')
    inst_ips = check_instance_status('ips', 'all')
    if len(inst_ids) != 0:
        terminate_instances(inst_ids, inst_ips)

    #run_experiment('m3.2xlarge', [8,16], glob.DATA_SET, glob.DATA_SET_BUCKET, 30, 'independent', 10, [0], 1, 16, '', [])
    # run_experiment('m3.2xlarge', [16], glob.DATA_SET, glob.DATA_SET_BUCKET, 30, 'independent', 10, [0], 0, 64, '-2016-08-17-16-37-44', ['m3.xlarge'])
    # run_experiment('m3.2xlarge', [8,16], glob.DATA_SET, glob.DATA_SET_BUCKET, 30, 'independent', 10, [0], 1, 16, '-2016-08-18-14-15-43', ['m3.2xlarge', 'm2.2xlarge', 'm3.xlarge'])
    # run_experiment('m3.2xlarge', [16], glob.DATA_SET, glob.DATA_SET_BUCKET, 30, 'independent', 10, [0], 0, 16, '-2016-08-17-16-37-44', ['m3.2xlarge', 'm2.2xlarge', 'm3.xlarge'])
    # run_experiment('m3.2xlarge', [2,4], glob.DATA_SET, glob.DATA_SET_BUCKET, 30, 'independent', 10, [0], 1, 16, '-2016-08-18-14-15-43', [])
    run_experiment('m3.2xlarge', [1], glob.DATA_SET, glob.DATA_SET_BUCKET, 30, 'independent', 10, [0], 0, 16, '-2016-08-17-16-37-44', [])
    run_experiment('m3.2xlarge', [1], glob.DATA_SET, glob.DATA_SET_BUCKET, 30, 'independent', 10, [0], 1, 16, '-2016-08-18-14-15-43', [])


if __name__ == "__main__":
    main()
