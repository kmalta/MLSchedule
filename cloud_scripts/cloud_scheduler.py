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




def run_suite_of_exps_static(scale_factor, scale_data, exp_folder, s3url, features, master_type, algorithm, replication, worker_type, epochs, trials, mach_count):
    jar_path = 'spark_job_files/log_reg_explicit_parallelism/target/scala-2.11/log-reg-explicit-parallelism_2.11-1.0.jar'

    print exp_folder

    scale = 1
    if scale_data == True:
        scale = scale_factor*mach_count


    suffix = time_str() + '_' + s3url.split('/')[-1] + '_' + str(mach_count) + '_machines_comm_data_' + str(epochs) + '_epochs_' + str(trials) + '_trials'

    print 'Num Exps:', str(trials)
    print 'Suffix:', suffix
    exps_array = [[scale,mach_count,epochs] for i in range(trials)]
    fixed_design_init(exp_folder, master_type, s3url, features, jar_path, algorithm, suffix, exps_array, replication, worker_type)




def run_real_suite(exp_fold, epochs, checkpoint, trials, scale_data, scale_factor, worker_type, machine_checkpoint, data, max_machs):
    machines = [i for i in range(machine_checkpoint, max_machs)]
    for mach_count in machines:
        if data == 1 or data == 0:
            run_suite_of_exps_static(scale_factor,scale_data, exp_fold, 's3://susy-data/susy', 18, worker_type, 'classification', 3, worker_type, epochs,trials, mach_count)
        if data == 2 or data == 0:
            run_suite_of_exps_static(scale_factor,scale_data, exp_fold, 's3://higgs-data/higgs', 28, worker_type, 'classification', 3, worker_type, epochs,trials, mach_count)
        if data == 3 or data == 0:
            run_suite_of_exps_static(scale_factor,scale_data, exp_fold, 's3://url-combined-data/url_combined', 3231961, worker_type, 'classification', 3, worker_type, epochs,trials, mach_count)
        if data == 4 or data == 0:
            run_suite_of_exps_static(scale_factor,scale_data, exp_fold, 's3://kdda-data/kdda', 20216830, worker_type, 'classification', 3, worker_type, epochs,trials, mach_count)
        if data == 5 or data == 0:
            run_suite_of_exps_static(scale_factor,scale_data, exp_fold, 's3://kddb-data/kddb', 29890095, worker_type, 'classification', 3, worker_type, epochs,trials, mach_count)

def machine_exp_synth_suite_sample(exp_folder, epochs, dataset, checkpoint, trials, scale_data, scale_factor, worker_type, machine_checkpoint, dataset_checkpoint):

    machines = [i for i in range(machine_checkpoint, 16)]
    for mach_count in machines:
        if mach_count > machines[0]:
            dataset_checkpoint = 1
        # if dataset_checkpoint <= 1:
        #     run_suite_of_exps_static(scale_factor,scale_data, exp_folder, 's3://synth-datasets-' + dataset + '/synth_data_1_0', 10, worker_type, 'classification', 3, worker_type, epochs,trials, mach_count)
        # if dataset_checkpoint <= 2:
        #     run_suite_of_exps_static(scale_factor,scale_data, exp_folder, 's3://synth-datasets-' + dataset + '/synth_data_2_0', 100, worker_type, 'classification', 3, worker_type, epochs,trials, mach_count)
        # if dataset_checkpoint <= 3:
        #     run_suite_of_exps_static(scale_factor,scale_data, exp_folder, 's3://synth-datasets-' + dataset + '/synth_data_3_0', 1000, worker_type, 'classification', 3, worker_type, epochs,trials, mach_count)
        # if dataset_checkpoint <= 4:
        #     run_suite_of_exps_static(scale_factor,scale_data, exp_folder, 's3://synth-datasets-' + dataset + '/synth_data_4_-1', 10000, worker_type, 'classification', 3, worker_type, epochs,trials, mach_count)
        # if dataset_checkpoint <= 5:
        #     run_suite_of_exps_static(scale_factor,scale_data, exp_folder, 's3://synth-datasets-' + dataset + '/synth_data_5_-2', int(1e5), worker_type, 'classification', 3, worker_type, epochs,trials, mach_count)
        # if dataset_checkpoint <= 6:
        #     run_suite_of_exps_static(scale_factor,scale_data, exp_folder, 's3://synth-datasets-' + dataset + '/synth_data_6_-3', int(1e6), worker_type, 'classification', 3, worker_type, epochs,trials, mach_count)
        if dataset_checkpoint <= 7:
            run_suite_of_exps_static(scale_factor,scale_data, exp_folder, 's3://synth-datasets-' + dataset + '/synth_data_7_-4', int(1e7), worker_type, 'classification', 3, worker_type, epochs,trials, mach_count)
        # if dataset_checkpoint <= 8:
        #     run_suite_of_exps_static(scale_factor,scale_data, exp_folder, 's3://synth-datasets-' + dataset + '/synth_data_8_-5', int(1e8), worker_type, 'classification', 3, worker_type, epochs,trials, mach_count)


def separate_machine_exp_synth_suite(exp_folds, epochs, dataset, checkpoint, trials, scale_data, scale_factor, machine_checkpoint, dataset_checkpoint):
    #machine_exp_synth_suite_sample(exp_folds[0], epochs, dataset, checkpoint, trials, scale_data, 8*scale_factor, 'hi1.4xlarge', machine_checkpoint, dataset_checkpoint)
    machine_exp_synth_suite_sample(exp_folds[1], epochs, dataset, checkpoint, trials, scale_data, 4*scale_factor, 'm1.large', machine_checkpoint, dataset_checkpoint)
    #machine_exp_synth_suite_sample(exp_folds[2], epochs, dataset, checkpoint, trials, scale_data, 4*scale_factor, 'cg1.4xlarge', machine_checkpoint, dataset_checkpoint)



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


    separate_machine_exp_synth_suite(['data/synth_cluster_scaling_exps_all_machine_counts_3_13_17_h1', 
                                      'data/synth_cluster_scaling_exps_all_machine_counts_3_13_17_m1',
                                      'data/synth_cluster_scaling_exps_all_machine_counts_3_13_17_c1'
                                     ],
                                      500,'2k', 0, 3, True, 0.05, 16, 1)

    run_real_suite('data/computation-scaling-3-30-17/m1', 500, 0, 1, False, 99.9, 'm1.large', 10, 3, 15)
    run_real_suite('data/computation-scaling-3-30-17/h1', 500, 0, 1, False, 99.9, 'hi1.4xlarge', 1, 0, 7)


    run_real_suite('data/computation-scaling-3-30-17/m1', 500, 0, 1, False, 99.9, 'm1.large', 1, 1, 15)
    run_real_suite('data/computation-scaling-3-30-17/m1', 500, 0, 1, False, 99.9, 'm1.large', 1, 2, 15)


    run_real_suite('data/computation-scaling-3-30-17/c1', 500, 0, 1, False, 99.9, 'cg1.4xlarge', 1, 3, 15)
    run_real_suite('data/computation-scaling-3-30-17/c1', 500, 0, 1, False, 99.9, 'cg1.4xlarge', 1, 1, 15)
    run_real_suite('data/computation-scaling-3-30-17/c1', 500, 0, 1, False, 99.9, 'cg1.4xlarge', 1, 2, 15)


if __name__ == "__main__":
    main()
