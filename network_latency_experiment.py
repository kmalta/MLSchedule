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



def run_experiment(master_inst_type, mach_array, data_set_name, data_bucket_name, runs, run_dependency, epochs, staleness):

    #CONSTANTS
    staleness = str(staleness)
    #Use this to only build the images

    master_ip, master_id = launch_instance_with_metadata(master_inst_type, 'master')
    add_ssh_key_to_master(master_ip)
    push_launch_script_to_master(master_ip)
    py_cmd_line('echo ' + master_ip + ' > host_master')

    local_file_dir = glob.DATA_SET_PATH + '/' + data_set_name + time_str()
    py_cmd_line('mkdir ' + local_file_dir)

    #INSTANCE TYPES AND FILTERS
    inst_types = read_instance_types()
    inst_types_filter = filter(lambda x: int(x[3]) > 13 , inst_types)
    #inst_types_filter = filter(lambda x: int(x[2]) > 4000, inst_types_filter)

    for inst_type in inst_types_filter:
        exp_dir = local_file_dir + '/' + inst_type[0]
        py_cmd_line('mkdir ' + exp_dir)
        for i in mach_array:
            old_ips = []

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
            # for ip in ips:
            #     add_swapfile(ip, inst_type[2])
                # py_scp_to_remote('', ip, '~/bosen/app/mlr/src/mlr_engine.cpp', 
                #                  glob.REMOTE_PATH + '/bosen/app/mlr/src/mlr_engine.cpp')
                # py_ssh('', ip, 'cd ' + glob.REMOTE_PATH + '/bosen/app/mlr; sudo make')

            mach_name_array = [inst_type[0] for k in range(len(ips))]
            num_chunks, num_digits, iters, chunk_parts, rem_chunk_parts = distribute_chunks(mach_name_array, data_bucket_name, data_set_name)

            file_creation_overhead_end_time = time()
            total_file_creation_overhead_time = file_creation_overhead_end_time - file_creation_overhead_start_time

            create_hostfiles_petuum_format(chunk_parts, ips)
            replace_hostfiles_petuum_format(master_ip)

            print num_chunks, num_digits, iters, chunk_parts, rem_chunk_parts
            #RUN MACHINE LEARNING JOB

            cores = str(inst_type[1])


            for j in range(runs):
                idx = 0
                for k in range(epochs):

                    out_file_name = ''
                    data_fetch_start_time = 0
                    data_fetch_end_time = 0
                    total_data_fetch_start_time = 0
                    machine_learning_start_time = 0
                    machine_learning_end_time = 0
                    total_machine_learning_time = 0

                    for it in range(iters - 1):
                        flag = 1
                        while flag ==1:
                            data_fetch_start_time = time()
                            data_fetch(chunk_parts, num_digits, idx, data_bucket_name, data_set_name)
                            flag = check_for_s3_403()
                            data_fetch_end_time = time()
                            total_data_fetch_time = data_fetch_end_time - data_fetch_start_time

                        machine_learning_start_time = time()
                        out_file_name = run_ml_task(master_ip, ips[0], inst_type, len(ips), str(k), cores, staleness, j, it, exp_dir, run_dependency)

                        idx += sum(chunk_parts)
                        kill_ml_task(master_ip)

                        machine_learning_end_time = time()
                        total_machine_learning_time = machine_learning_end_time - machine_learning_start_time

                        write_times(total_spin_up_time, total_file_creation_overhead_time, 
                                   total_data_fetch_time, total_machine_learning_time, out_file_name)


                    #If there is no remainder, we just use the original chunk array
                    if list(set(rem_chunk_parts)) != [0]:
                        create_hostfiles_petuum_format(rem_chunk_parts, ips)
                        replace_hostfiles_petuum_format(master_ip)

                        data_fetch_start_time = time()
                        data_fetch(rem_chunk_parts, num_digits, idx, data_bucket_name, data_set_name)
                        data_fetch_end_time = time()
                        total_data_fetch_time = data_fetch_end_time - data_fetch_start_time

                        machine_learning_start_time = time()
                        out_file_name = run_ml_task(master_ip, ips[0], inst_type, len(ips), str(k), cores, staleness, j, iters - 1, exp_dir, run_dependency)

                        create_hostfiles_petuum_format(chunk_parts, ips)
                        replace_hostfiles_petuum_format(master_ip)
                    else:
                        if iters > 1 or (k == 0 and j == 0):
                            data_fetch_start_time = time()
                            data_fetch(chunk_parts, num_digits, idx, data_bucket_name, data_set_name)
                            data_fetch_end_time = time()
                            total_data_fetch_time = data_fetch_end_time - data_fetch_start_time

                        machine_learning_start_time = time()
                        out_file_name = run_ml_task(master_ip, ips[0], inst_type, len(ips), str(k), cores, staleness, j, iters - 1, exp_dir, run_dependency)



                    kill_ml_task(master_ip)

                    machine_learning_end_time = time()
                    total_machine_learning_time = machine_learning_end_time - machine_learning_start_time

                    write_times(total_spin_up_time, total_file_creation_overhead_time, 
                               total_data_fetch_time, total_machine_learning_time, out_file_name)

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

    run_experiment('m3.2xlarge', [1,2,4,8,16], glob.DATA_SET, glob.DATA_SET_BUCKET, 30, 'independent', 20, 5)
    #staleness_experiment('m2.2xlarge', 'm3.2xlarge', [4,8,16], [1,2,4,8,16,100000000], glob.DATA_SET, glob.DATA_SET_BUCKET, 1, 'independent')

if __name__ == "__main__":
    main()
