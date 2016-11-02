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

from get_bid_API import *
from configure_hadoop_and_spark import *

region = 'us-west-1'


def fixed_design_init(master_type, s3url, num_features, jar_path, algorithm, init_time_str, exps_array, replication, worker_type, part_way, cliff):

    experiments = exps_array

    if cliff == True:
        master_ip, master_id = start_master_node(master_type)
        configure_experiment_machines(experiments, worker_type, master_ip, master_id, s3url)

    if part_way == False:
        master_ip, master_id = start_master_node(master_type)
        configure_experiment_machines(experiments, worker_type, master_ip, master_id, s3url)
        os.system('mkdir aristotle_all_in_one_exps/experiment' + init_time_str)
        os.system('echo ' + s3url + ' >> aristotle_all_in_one_exps/experiment' + init_time_str + '/experiment')
        os.system('echo ' + repr(experiments[0]) + ' >> aristotle_all_in_one_exps/experiment' + init_time_str + '/experiment')
        log_idx = 0
    else:
        master_ip, master_id = start_master_node(master_type)
        configure_experiment_machines(experiments, worker_type, master_ip, master_id, s3url)
        f = open('host_master', 'r')
        master_ip = f.read().strip()
        f.close()
        f = open('aristotle_all_in_one_exps/experiment' + init_time_str + '/all_iters', 'r')
        log_idx = len(f.readlines())
        f.close()


    time_array = []

    prev_exp = None
    second_prev = None
    prev_exp_percent = 0


    f2 = open('aristotle_all_in_one_exps/experiment' + init_time_str + '/log_metrics_file', 'a')
    meaning_array = [
                     'first at GeneralizedLinearAlgorithm.scala:246', 
                     'count at DataValidators.scala:40', 
                     'count at GradientDescent.scala:209',
                     'treeAggregate at GradientDescent.scala:239',
                    ]
    f2.write('\t'.join(meaning_array) + '\n')
    f2.close()

    os.system('mkdir aristotle_all_in_one_exps/experiment' + init_time_str + '/logs')
    for k, exp in enumerate(experiments):
        log_path = 'aristotle_all_in_one_exps/experiment' + init_time_str + '/logs/log_trial_' + str(k + log_idx)
        if os.path.isfile(log_path):
            os.system('rm ' + log_path)

        print "\n\n#######################################################"
        print "#######################################################\n\n"
        print "@@EXPERIMENT", repr(exp)
        print "\n\n#######################################################"
        print "#######################################################\n\n"
        inst_ips = check_instance_status('ips', 'all')
        nodes_info = get_all_hosts(master_ip, inst_ips)
        sub_nodes_info = nodes_info[:2**exp[1] + 1]
        try:
            prev_exp_percent = prev_exp[0]
        except:
            prev_exp_percent = 0
        
        if prev_exp == None:
            configure_and_run_experiment_frameworks('experiment', num_features, exp, sub_nodes_info, s3url, jar_path, algorithm, exp[2], True, prev_exp_percent, replication, log_path)
        else:
            configure_and_run_experiment_frameworks('experiment', num_features, exp, sub_nodes_info, s3url, jar_path, algorithm, exp[2], False, prev_exp_percent, replication, log_path)


        print "\n\n#######################################################"
        print "#######################################################\n\n"
        print "@@END EXPERIMENT", repr(exp)
        print "\n\n#######################################################"
        print "#######################################################\n\n"

        metrics = read_job_time(master_ip)
        #SOMEHOW GET METRICS
        print "METRICS: ", repr(metrics)

        os.system('echo ' + str(metrics[0]) + ' >> aristotle_all_in_one_exps/experiment' + init_time_str + '/setup')
        os.system('echo ' + str(metrics[1]) + ' >> aristotle_all_in_one_exps/experiment' + init_time_str + '/load_data')
        os.system('echo ' + str(metrics[2]) + ' >> aristotle_all_in_one_exps/experiment' + init_time_str + '/init_vector')
        os.system('echo ' + str(metrics[3]) + ' >> aristotle_all_in_one_exps/experiment' + init_time_str + '/model_setup')
        os.system('echo ' + str(metrics[4]) + ' >> aristotle_all_in_one_exps/experiment' + init_time_str + '/all_iters')
        os.system('python parse_spark_log_for_times.py aristotle_all_in_one_exps/experiment' + init_time_str + ' ' + str(k))
        second_prev = prev_exp
        prev_exp = exp




def time_str():
    return strftime("-%Y-%m-%d-%H-%M-%S", gmtime())


def get_cost(num_machines, inst_type):
    ret_dict = get_bid(inst_type, region)
    return ret_dict['bid']*num_machines

def design_experiments(budget, duration, training_budget, duration_budget, inst_types):

    inst_type_experiments = []
    for exp_count, inst_type in enumerate(inst_types):
        scales = [2**i for i in range(1,11)]
        machs = []
        i = 1
        while get_cost(2**i, inst_type) <= budget*training_budget:
            machs.append(i)
            i += 1

        if machs == []:
            print 'The instance type selected was too expensive to run an experiment on our budget.  Skipping.'
            continue

        exps = []
        for i in range(len(scales)):
            for j in range(len(machs)):
                exps.append([1, scales[i], machs[j], 2**machs[j]])
        print '\nExperiment', str(exp_count) + ':\t' + inst_type + '\n'
        optimal_experiments = A_optimal_experimental_design(exps, training_budget*budget, duration_budget*duration)
        for i in range(len(optimal_experiments)):
            optimal_experiments[i] = optimal_experiments[i][1:-1]
            optimal_experiments[i][0] = float(100)/optimal_experiments[i][0]
        inst_type_experiments.append(optimal_experiments)
    return inst_type_experiments

def A_optimal_experimental_design(exps, B_init, T_init):

    num_exps = len(exps)

    # Turn experiments list into a cvx matrix
    A = cvx.matrix(exps)

    # print experiments list

    # Create new problem
    prob =pic.Problem()

    A = pic.new_param('A', A)

    lamb = prob.add_variable('lamb',num_exps, lower=0)
    targ = prob.add_variable('targ',1, lower=0)


    # Bounds lambda between 0 and 1 for each var in lambda
    #prob.add_list_of_constraints( [abs(lamb[i]) <= 1 for i in range(num_exps)], 'i', '[num_exps]')
    prob.add_constraint(1 | lamb < 1)

    # Creates monitary and duration constraints on initialization problem
    #prob.add_constraint(pic.sum([lamb[i]*exps[i][0] for i in range(num_exps)], 'i') <= T_init)

    # Creates covariance matrix
    cov_mat = pic.sum([lamb[i]*A[i]*A[i].T for i in range(num_exps)],'i', '[num_exps]')

    prob.add_constraint(targ > pic.tracepow(cov_mat,-1))
    prob.set_objective('min',targ)


    #print prob
    #print 'type:   ' + prob.type
    #print 'status: ' + prob.status
    prob.solve(verbose=0)
    #print 'status: ' + prob.status

    lamb=lamb.value
    w=lamb/sum(lamb) #normalize mu to get the optimal weights
    #print
    #print 'The optimal design is:'
    #print repr(list(w))
    filtered_exps = filter(lambda x: x[1] > 1e-3, enumerate(w))
    ret_exps = []
    for idx in filtered_exps:
        print repr(exps[idx[0]]), str(float(idx[1])*100) + '%'
        ret_exps.append(exps[idx[0]])
    return ret_exps




def check_part_way(part_way, suffix):
    if part_way == False:
        os.system('rm current_exp_path')
        f = open('current_exp_path', 'w')
        f.write('aristotle_all_in_one_exps/experiment' + suffix)
        f.close()
        return 30, suffix
    else:
        f = open('current_exp_path', 'r')
        path = f.read().strip()
        f.close()
        if os.path.isfile(path + '/all_iters'):
            f = open(path + '/all_iters', 'r')
            num_run = len(f.readlines())
            f.close()
        else:
            num_run = 0
        new_path = '-'.join(path.split('-')[1:])

        f = open(path + '/check_abnormalities', 'a')
        f.write('\t'.join([new_path, str(num_run + 1)]) + '\n')
        f.close()
        return 30 - num_run, '-' + new_path


def run_suite_of_exps(s3url, features, master_type, first, second, algorithm, replication, worker_type, part_way, skip, cliff):

    print part_way

    if skip == True:
        return

    if first == True:
        for i in range(1,2):
            suffix = time_str() + '_' + s3url.split('/')[-1] + '_1_machine_500_epochs_30_trials'
            num_exps, suffix = check_part_way(part_way, suffix)
            print 'Num Exps:', str(num_exps)
            print 'Suffix:', suffix
            exps_array = [[6.25,0,500] for i in range(num_exps)]
            fixed_design_init(master_type, s3url, features, 'spark_test/target/scala-2.11/ml-test_2.11-1.0.jar', algorithm, suffix, exps_array, replication, worker_type, part_way, cliff)
            part_way = False
            os.system('rm current_exp_path')

    if second == True:
        for i in range(1,2):
            suffix = time_str() + '_' + s3url.split('/')[-1] +  '_2_machines_500_epochs_30_trials'
            num_exps, suffix = check_part_way(part_way, suffix)
            print 'Num Exps:', str(num_exps)
            print 'Suffix:', suffix
            exps_array = [[12.5,1,500] for i in range(num_exps)]
            fixed_design_init(master_type, s3url, features, 'spark_test/target/scala-2.11/ml-test_2.11-1.0.jar', algorithm, suffix, exps_array, replication, worker_type, part_way, cliff)
            part_way = False
            os.system('rm current_exp_path')

    for i in range(1,2):
        suffix = time_str() + '_' + s3url.split('/')[-1] +  '_16_machines_500_epochs_30_trials'
        num_exps, suffix = check_part_way(part_way, suffix)
        print 'Num Exps:', str(num_exps)
        print 'Suffix:', suffix
        exps_array = [[100,4,500] for i in range(num_exps)]
        fixed_design_init(master_type, s3url, features, 'spark_test/target/scala-2.11/ml-test_2.11-1.0.jar', algorithm, suffix, exps_array, replication, worker_type, part_way, cliff)
        part_way = False
        os.system('rm current_exp_path')



datasets = ['higgs', 'susy', 'url', 'kddb', 'kdda']


def main():
    glob.set_globals()


    try:
        cla = sys.argv
        if cla[1] == 'restart':
            os.system('rm current_exp_path')
        if cla[1] == 'one_off':
            os.system('rm current_exp_path')
            run_suite_of_exps('s3://higgs-data/higgs', 28, 'cg1.4xlarge', False, False, 'classification', 3, 'cg1.4xlarge', False, False, False)
            sys.exit("We out!")

        # if cla[1] == 'start_at_cliff':
        #     f = open('current_exp_path', 'w')

        #     #manually change this
        #     suffix = time_str() + '_' + 'susy' + '_2_machines_500_epochs_30_trials'
        #     f.write('aristotle_all_in_one_exps/experiment' + suffix)
        #     f.close()
        #     #manually change this
        #     run_suite_of_exps('s3://susy-data/susy', 18, 'cg1.4xlarge', False, True, 'classification', 3, 'cg1.4xlarge', True, False, True)

            
    except:
        1

    if os.path.isfile('current_exp_path'):

        f = open('current_exp_path', 'r')
        path = f.read().strip()
        arr = path.split('-')[-1].split('_')
        dataset = arr[1]
        machs = arr[2]

        idx = datasets.index(dataset)
        partial = [False for i in range(5)]
        partial[idx] = True
        skip = []
        if idx == 0:
            skip = [False for i in range(5)]
        else:
            skip = [True for i in range(idx)] + [False for i in range(5 - idx)]

        one_mach_exp = [True for i in range(5)]
        two_mach_exp = [True for i in range(5)]

        if machs == '2':
            one_mach_exp[idx] = False
        if machs == '16':
            one_mach_exp[idx] = False
            two_mach_exp[idx] = False

        print one_mach_exp
        print two_mach_exp
        print partial
        print skip

        run_suite_of_exps('s3://higgs-data/higgs', 28, 'cg1.4xlarge', one_mach_exp[0], two_mach_exp[0], 'classification', 3, 'cg1.4xlarge', partial[0], skip[0], False)
        run_suite_of_exps('s3://susy-data/susy', 18, 'cg1.4xlarge', one_mach_exp[1], two_mach_exp[1], 'classification', 3, 'cg1.4xlarge', partial[1], skip[1], False)
        run_suite_of_exps('s3://url-combined-data/url_combined', 3231961, 'cg1.4xlarge', one_mach_exp[2], two_mach_exp[2], 'classification', 3, 'cg1.4xlarge', partial[2], skip[2], False)
        run_suite_of_exps('s3://kddb-data/kddb', 29890095, 'm1.large', one_mach_exp[3], two_mach_exp[3], 'classification', 3, 'm1.large', partial[3], skip[3], False)
        run_suite_of_exps('s3://kdda-data/kdda', 20216830, 'm1.large', one_mach_exp[4], two_mach_exp[4], 'classification', 3, 'm1.large', partial[4], skip[4], False)

    else:
        # run_suite_of_exps('s3://higgs-data/higgs', 28, 'cg1.4xlarge', True, True, 'classification', 3, 'cg1.4xlarge', False, False, False)
        # run_suite_of_exps('s3://susy-data/susy', 18, 'cg1.4xlarge', True, True, 'classification', 3, 'cg1.4xlarge', False, False, False)
        run_suite_of_exps('s3://url-combined-data/url_combined', 3231961, 'cg1.4xlarge', True, True, 'classification', 3, 'cg1.4xlarge', False, False, False)
        # run_suite_of_exps('s3://kddb-data/kddb', 29890095, 'm1.large', True, True, 'classification', 3, 'm1.large', False, False, False)
        #run_suite_of_exps('s3://kdda-data/kdda', 20216830, 'm1.large', True, True, 'classification', 3, 'm1.large', False, False, False)






if __name__ == "__main__":
    main()
