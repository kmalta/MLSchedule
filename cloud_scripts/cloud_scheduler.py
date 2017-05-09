from glob import *
from scipy.optimize import nnls
import numpy as np
import math
import cvxopt as cvx
import picos as pic
import sys
from time import sleep, gmtime, strftime, time
import os
import ast
import multipolyfit

from configure_hadoop_and_spark import *



def fixed_design_init(exp_folder, master_type, s3url, num_features, jar_path, algorithm, init_time_str, exps_array, replication, worker_type):

    experiments = exps_array


    master_ip, master_id = start_master_node(master_type)
    configure_experiment_machines(experiments, worker_type, master_ip, master_id, s3url)
    os.system('mkdir ' + exp_folder + '/experiment' + init_time_str)
    os.system('echo ' + s3url + ' >> ' + exp_folder + '/experiment' + init_time_str + '/experiment')
    os.system('echo ' + repr(experiments[0]) + ' >> ' + exp_folder + '/experiment' + init_time_str + '/experiment')
    os.system('echo master_type ' + master_type + ' >> ' + exp_folder + '/experiment' + init_time_str + '/experiment')
    os.system('echo worker_type ' + worker_type + ' >> ' + exp_folder + '/experiment' + init_time_str + '/experiment')
    log_idx = 0


    time_array = []

    prev_exp = None
    second_prev = None
    prev_exp_percent = 0


    os.system('mkdir ' + exp_folder + '/experiment' + init_time_str + '/logs')
    for k, exp in enumerate(experiments):
        log_path =  exp_folder + '/experiment' + init_time_str + '/logs/log_trial_' + str(k + log_idx)

        print "\n\n#######################################################"
        print "#######################################################\n\n"
        print "@@EXPERIMENT", repr(exp)
        print "\n\n#######################################################"
        print "#######################################################\n\n"
        inst_ips = check_instance_status('ips', 'all')
        nodes_info = get_all_hosts(master_ip, inst_ips)
        if len(nodes_info[1:]) != experiments[0][1]:
            sys.exit("We do not have all of our nodes up!")

        sub_nodes_info = nodes_info[:2**exp[1] + 1]
        try:
            prev_exp_percent = prev_exp[0]
        except:
            prev_exp_percent = 0
        
        if prev_exp == None:
            configure_and_run_experiment_frameworks('experiment', num_features, exp, sub_nodes_info, s3url, jar_path, algorithm, exp[2], True, prev_exp_percent, replication, log_path, worker_type)
        else:
            configure_and_run_experiment_frameworks('experiment', num_features, exp, sub_nodes_info, s3url, jar_path, algorithm, exp[2], False, prev_exp_percent, replication, log_path, worker_type)


        print "\n\n#######################################################"
        print "#######################################################\n\n"
        print "@@END EXPERIMENT", repr(exp)
        print "\n\n#######################################################"
        print "#######################################################\n\n"

        metrics = read_job_time(master_ip)

        print "METRICS: ", repr(metrics)

        os.system('echo ' + str(metrics[4]) + ' >> ' + exp_folder + '/experiment' + init_time_str + '/all_iters')
        os.system('python cloud_scripts/parse_spark_log_for_times.py ' + exp_folder + '/experiment' + init_time_str + ' ' + str(k))
        second_prev = prev_exp
        prev_exp = exp




def run_suite_of_exps_static(samples_per_machine, scale_data, exp_folder, s3url, features, master_type, algorithm, replication, worker_type, epochs, trials, mach_count, scale_type, fraction):
    jar_path = 'spark_job_files/log_reg_explicit_parallelism/target/scala-2.11/log-reg-explicit-parallelism_2.11-1.0.jar'

    print exp_folder

    total_samples = 0
    suffix = ''
    if scale_data == True:
        if scale_type == 'sample':
            total_samples = samples_per_machine*mach_count
            suffix = '_comm'
        if scale_type == 'fractional':
            total_samples = samples[datasets.index(s3url.split('/')[-1].split('_')[0])]
            total_samples = int(fraction*total_samples*.999)
            suffix = '_' + str(fraction) + '_fraction'


    suffix = time_str() + '_' + s3url.split('/')[-1] + '_' + str(mach_count) + '_machines' + suffix

    print 'Num Exps:', str(trials)
    print 'Suffix:', suffix
    exps_array = [[total_samples,mach_count,epochs] for i in range(trials)]
    fixed_design_init(exp_folder, master_type, s3url, features, jar_path, algorithm, suffix, exps_array, replication, worker_type)




def run_real_suite(exp_fold, epochs, checkpoint, trials, scale_data, samples_per_machine, scale_type, fraction, worker_type, machine_checkpoint, data, max_machs):
    reset = 5
    if data < 0:
        reset = int(math.fabs(data))
        data = 0

    machines = []
    machines = [i for i in range(machine_checkpoint, max_machs + 1)]
    # if fraction == 1:
    #     machines = [i for i in range(machine_checkpoint, max_machs + 1)]
    # else:
    #     machines = [2**i for i in range(machine_checkpoint,3)]
    for mach_count in machines:
        if (data == 1 or data <= 0) and reset >= 5:
            run_suite_of_exps_static(samples_per_machine,scale_data, exp_fold, 's3://susy-data/susy', 18, worker_type, 'classification', 3, worker_type, epochs,trials, mach_count, scale_type, fraction)
        if (data == 2 or data <= 0) and reset >= 4:
            run_suite_of_exps_static(samples_per_machine,scale_data, exp_fold, 's3://higgs-data/higgs', 28, worker_type, 'classification', 3, worker_type, epochs,trials, mach_count, scale_type, fraction)
        if (data == 3 or data <= 0) and reset >= 3:
            run_suite_of_exps_static(samples_per_machine,scale_data, exp_fold, 's3://url-combined-data/url_combined', 3231961, worker_type, 'classification', 3, worker_type, epochs,trials, mach_count, scale_type, fraction)
        if (data == 4 or data <= 0) and reset >= 2:
            run_suite_of_exps_static(samples_per_machine,scale_data, exp_fold, 's3://kdda-data/kdda', 20216830, worker_type, 'classification', 3, worker_type, epochs,trials, mach_count, scale_type, fraction)
        # if (data == 5 or data <= 0) and reset >= 1:
        #     run_suite_of_exps_static(samples_per_machine,scale_data, exp_fold, 's3://kddb-data/kddb', 29890095, worker_type, 'classification', 3, worker_type, epochs,trials, mach_count, scale_type, fraction)

        if reset < 5:
            reset = 5

def machine_exp_synth_suite_sample(exp_fold, epochs, dataset, checkpoint, trials, scale_data, samples_per_machine, scale_type, fraction, worker_type, machine_checkpoint, data, max_machs):

    reset = 5
    if data < 0:
        reset = int(math.fabs(data))
        data = 0

    machines = [i for i in range(machine_checkpoint, max_machs + 1)]
    for mach_count in machines:
        if mach_count > machines[0]:
            data = 0
            # if single == True:
            #     dataset_checkpoint = 9
            # else:
            #     dataset_checkpoint = 1
        if data == 1:
            run_suite_of_exps_static(samples_per_machine,scale_data, exp_fold, 's3://synth-datasets-' + dataset + '/synth_data_1_0', 10, worker_type, 'classification', 3, worker_type, epochs,trials, mach_count, scale_type, fraction)
        if data == 2:
            run_suite_of_exps_static(samples_per_machine,scale_data, exp_fold, 's3://synth-datasets-' + dataset + '/synth_data_2_0', 100, worker_type, 'classification', 3, worker_type, epochs,trials, mach_count, scale_type, fraction)
        # if dataset_checkpoint <= 3:
        #     run_suite_of_exps_static(samples_per_machine,scale_data, exp_folder, 's3://synth-datasets-' + dataset + '/synth_data_3_0', 1000, worker_type, 'classification', 3, worker_type, epochs,trials, mach_count)
        # if dataset_checkpoint <= 4:
        #     run_suite_of_exps_static(samples_per_machine,scale_data, exp_folder, 's3://synth-datasets-' + dataset + '/synth_data_4_-1', 10000, worker_type, 'classification', 3, worker_type, epochs,trials, mach_count)
        # if dataset_checkpoint <= 5:
        #     run_suite_of_exps_static(samples_per_machine,scale_data, exp_folder, 's3://synth-datasets-' + dataset + '/synth_data_5_-2', int(1e5), worker_type, 'classification', 3, worker_type, epochs,trials, mach_count)
        if data == 6:
            run_suite_of_exps_static(samples_per_machine,scale_data, exp_fold, 's3://synth-datasets-' + dataset + '/synth_data_6_-3', int(1e6), worker_type, 'classification', 3, worker_type, epochs,trials, mach_count, scale_type, fraction)
        if data == 7:
            run_suite_of_exps_static(samples_per_machine,scale_data, exp_fold, 's3://synth-datasets-' + dataset + '/synth_data_7_-4', int(1e7), worker_type, 'classification', 3, worker_type, epochs,trials, mach_count, scale_type, fraction)
        # if dataset_checkpoint <= 8:
        #     run_suite_of_exps_static(samples_per_machine,scale_data, exp_folder, 's3://synth-datasets-' + dataset + '/synth_data_8_-5', int(1e8), worker_type, 'classification', 3, worker_type, epochs,trials, mach_count)



#REFERENCE VALUES
datasets = ['higgs', 'susy', 'url', 'kdda', 'kddb']
datasets_bytes = [7.4, 2.3, 2.1, 2.5, 4.8]
samples = [11000000,5000000,2396130,8407752,19264097]
features = [28,18,3231961,20216830,29890095]


def main():
    glob.set_globals()


    pid = str(os.getpid())
    print "PID:", pid

    os.system('python cloud_scripts/text_message_warn.py ' + pid + ' &')

    #run_real_suite('data/real_fractional_times_30_trials/m1', 500, 0, 30, True, 4, 'fractional', .5**4, 'm1.large', 1, 0, 15)
    #run_real_suite('data/real_comm_times_30_trials/m1', 500, 0, 30, True, 4, 'sample', 1, 'm1.large', 3, 0, 3)
    #run_real_suite('data/real_comm_times_30_trials/m1', 500, 0, 30, True, 4, 'sample', 1, 'm1.large', 13, 1, 15)

    #run_real_suite('data/real_comm_times/r1', 500, 0, 1, True, 8, 'sample', 1, 'm2.4xlarge', 2, 4, 2)
    #run_real_suite('data/real_comm_times/r1', 500, 0, 1, True, 8, 'sample', 1, 'm2.4xlarge', 2, 5, 2)
    

    #run_real_suite('data/real_comm_times/r1', 500, 0, 1, True, 8, 'sample', 1, 'm2.4xlarge', 3, 5, 8)

    #run_real_suite('data/real_times/r1', 500, 0, 1, False, 8, 'sample', 1, 'm2.4xlarge', 5, 5, 8)
    # run_real_suite('data/real_times/m1', 500, 0, 1, False, 4, 'sample', 1, 'm1.large', 16, 0, 16)
    # run_real_suite('data/real_comm_times/m1', 500, 0, 1, True, 4, 'sample', 1, 'm1.large', 16, 0, 16)
    # run_real_suite('data/real_times_30_trials/m1', 500, 0, 30, False, 4, 'sample', 1, 'm1.large', 2, 0, 2)
    # for i in range(17, 20):
    #     run_real_suite('data/real_times/m1', 500, 0, 1, False, 4, 'sample', 1, 'm1.large', i, 0, i)
    #     run_real_suite('data/real_comm_times/m1', 500, 0, 1, True, 4, 'sample', 1, 'm1.large', i, 0, i)
    # run_real_suite('data/real_times_30_trials/m1', 500, 0, 30, False, 4, 'sample', 1, 'm1.large', 3, 3, 3)
    # for i in [4,5,6,7,8]:
    #     run_real_suite('data/real_times_30_trials/m1', 500, 0, 30, False, 4, 'sample', 1, 'm1.large', i, 0, i)
    

    #run_real_suite('data/real_times_30_trials/m1', 500, 0, 30, False, 8, 'sample', 1, 'm1.large', 2, 0, 8)
    # for frac in [round(float(2)/i,4) for i in range(2,17)]:
    #     print frac

    # for frac in [round(float(2)/i,4) for i in range(3,17)]:
    #     run_real_suite('data/real_fractional_times/m1', 500, 0, 1, False, 4, 'fractional', frac, 'm1.large', 2, 1, 2)
    #     run_real_suite('data/real_fractional_times/m1', 500, 0, 1, False, 4, 'fractional', frac, 'm1.large', 2, 2, 2)
    #     run_real_suite('data/real_fractional_times/m1', 500, 0, 1, False, 4, 'fractional', frac, 'm1.large', 2, 3, 2)




    # run_real_suite('data/real_comm_times_30_trials/m1', 500, 0, 30, True, 4, 'sample', 1, 'm1.large', 16, 1, 16)
    # run_real_suite('data/real_comm_times_30_trials/m1', 500, 0, 30, True, 4, 'sample', 1, 'm1.large', 14, 2, 16)







    # for i in [4]:
    #     run_real_suite('data/hour_long_experiments/m1', 30*500, 0, 1, False, 4, 'sample', 1, 'm1.large', i, 1, i)

    # for i in [4]:
    #     run_real_suite('data/hour_long_experiments/m1', 15*500, 0, 1, False, 4, 'sample', 1, 'm1.large', i, 2, i)


    # run_real_suite('data/hour_long_comm/m1', 15000, 0, 1, True, 4, 'sample', 1, 'm1.large', 4, 1, 4)
    # machine_exp_synth_suite_sample('data/hour_long_synth_comm/m1', 15000, '2k', 0, 1, True, 4, 'sample', 1, 'm1.large', 4, 2, 4)
    #run_real_suite('data/hour_long_comm/m1', 1250, 0, 1, True, 4, 'sample', 1, 'm1.large', 4, 3, 4)
    #run_real_suite('data/hour_long_full/m1', 8000, 0, 1, False, 4, 'sample', 1, 'm1.large', 4, 2, 4)

    #run_real_suite('data/hour_long_full/m1', 1250, 0, 1, False, 4, 'sample', 1, 'm1.large', 4, 3, 4)
    for i in range(1,17):
        for k in range(1,6):
            machine_exp_synth_suite_sample('data/hour_long_synth_comm/c1', 200000, '2k', 0, 1, True, 4, 'sample', 1, 'cg1.4xlarge', i, k, i)


    for i in range(1,17):
        for k in range(1,6):
            machine_exp_synth_suite_sample('data/hour_long_synth_comm/m1', 200000, '2k', 0, 1, True, 4, 'sample', 1, 'cg1.4xlarge', i, k, i)


    for i in range(1,17):
        for k in range(6,7):
            machine_exp_synth_suite_sample('data/hour_long_synth_comm/m1', 12000, '2k', 0, 1, True, 4, 'sample', 1, 'cg1.4xlarge', i, k, i)


    for i in range(1,17):
        for k in range(7,8):
            machine_exp_synth_suite_sample('data/hour_long_synth_comm/m1', 1800, '2k', 0, 1, True, 4, 'sample', 1, 'cg1.4xlarge', i, k, i)

    # for i in range(1,17):
    #     for k, epochs in enumerate([12000, ]):
    #         machine_exp_synth_suite_sample('data/hour_long_comm/c1', k, '2k', 0, 1, True, 4, 'sample', 1, 'cg1.4xlarge', i, k, i)


    #machine_exp_synth_suite_sample('data/hour_long_synth_comm/m1', 1250, '2k', 0, 1, True, 4, 'sample', 1, 'm1.large', 4, 7, 4)
    #machine_exp_synth_suite_sample('data/hour_long_synth_comm/m1', 15000, '2k', 0, 1, True, 4, 'sample', 1, 'm1.large', 4, 1, 4)
    #machine_exp_synth_suite_sample('data/hour_long_synth_comm/m1', 1250, '2k', 0, 1, True, 4, 'sample', 1, 'm1.large', 4, 6, 4)





    # for k in range(2,17):
    #     run_real_suite('data/real_comm_times_30_trials/m1', 500, 0, 10, True, 4, 'sample', 1, 'm1.large', k, 4, k)
    #     run_real_suite('data/real_times_30_trials/m1', 500, 0, 10, False, 4, 'sample', 1, 'm1.large', k, 4, k)
    #     run_real_suite('data/real_fractional_times_30_trials/m1', 500, 0, 10, True, 4, 'fractional', round(float(2)/k,4), 'm1.large', 2, 4, 2)

            #run_real_suite('data/real_fractional_times_30_trials/m1', 500, 0, 30, True, 4, 'fractional', i, 'm1.large', 2, 3, 2)

    #machine_exp_synth_suite_sample('data/synth_comm_times_30_trials/m1', 500, '2k', 0, 10, True, 4, 'sample', 1, 'm1.large', 9, 2, 9)
    #machine_exp_synth_suite_sample('data/synth_comm_times_30_trials/m1', 500, '2k', 0, 10, True, 4, 'sample', 1, 'm1.large', 9, 2, 9)
    # for i in range(10, 17):
    #     machine_exp_synth_suite_sample('data/synth_comm_times_30_trials/m1', 500, '2k', 0, 10, True, 4, 'sample', 1, 'm1.large', i, 0, i)


    #machine_exp_synth_suite_sample('data/synth_comm_times_30_trials/m1', 500, '2k', 0, 10, True, 4, 'sample', 1, 'm1.large', 10, 1, 10)
    # for i in range(2, 17):
    #     machine_exp_synth_suite_sample('data/synth_comm_times_30_trials/m1', 500, '2k', 0, 10, True, 4, 'sample', 1, 'm1.large', i, 0, i)







if __name__ == "__main__":
    main()
