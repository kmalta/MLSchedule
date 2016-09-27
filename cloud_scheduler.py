from glob import *
from scipy.optimize import nnls
import numpy as np
import math
import cvxopt as cvx
import picos as pic
import sys
from time import sleep, gmtime, strftime, time
import os

from get_bid_API import *
from configure_hadoop_and_spark import *

region = 'us-west-1'
inst_types = ['m3.xlarge']#, 'm2.2xlarge', 'm3.2xlarge']

def init(s3url, num_features, budget, duration, training_budget, duration_budget, jar_path, algorithm):
    
    experiments = design_experiments(budget, duration, training_budget, duration_budget, inst_types)
    print repr(experiments)

    init_time_str = time_str()
    # Run Experiments from Optimal Experimental Design
    #ADD IN PAY MECHANISM SOME WAY
    #master_ip, master_id = start_master_node(master_type)
    master_ip = '128.111.179.173'

    all_model_coeffs = []
    all_actual_times = []

    for i, experiment in enumerate(experiments):
        #configure_experiment_machines(experiment, inst_types[i], master_ip, master_id, s3url)


        time_array = []
        actual_times_array = []
        #SPLIT DATA FOR EXP
        for exp in experiment:
            print "\n\n#######################################################"
            print "#######################################################\n\n"
            print repr(exp)
            print "\n\n#######################################################"
            print "#######################################################\n\n"
            inst_ips = check_instance_status('ips', 'all')
            nodes_info = get_all_hosts(master_ip, inst_ips)
            sub_nodes_info = nodes_info[:2**exp[1] + 1]
            configure_and_run_experiment_frameworks('experiment', num_features, exp, sub_nodes_info, s3url, jar_path, algorithm)

            metric = read_job_time(master_ip)
            #SOMEHOW GET METRICS
            print "METRIC: ", str(metric)
            if metric != None:
                time_array.append(metric)


            configure_and_run_experiment_frameworks('actual', num_features, exp, sub_nodes_info, s3url, jar_path, algorithm)

            metric = read_job_time(master_ip)
            print "ACTUAL METRIC: ", str(metric)
            if metric != None:
                actual_times_array.append(metric)


        model_coeffs = list(fit_model(experiment, time_array)[0])
        all_model_coeffs.append(model_coeffs)
        all_actual_times.append(actual_times_array)

        predicted_vs_actual(model_coeffs, experiment, actual_times_array)

    print repr(all_model_coeffs)
    os.system('mkdir init_experiments/experiment' + init_time_str)
    f = open('init_experiments/experiment' + init_time_str + '/model_coeffs', 'w')
    f.write(repr(model_coeffs) + '\n')
    f.close()
    f = open('init_experiments/experiment' + init_time_str + '/actual_times', 'w')
    f.write(repr(all_actual_times) + '\n')
    f.close()

def time_str():
    return strftime("-%Y-%m-%d-%H-%M-%S", gmtime())

def fit_model(experiment_array, time_array):
    exp_list = []
    for elem in experiment_array:
        exp_arr = [1, float(elem[0])/(2**elem[1]), elem[1], 2**elem[1]]
        exp_list.append(np.asarray(exp_arr))
    exp_mat = np.asarray(exp_list)
    nnls_coeffs = nnls(exp_mat, np.asarray(time_array))
    return nnls_coeffs

def predicted_vs_actual(model_coeffs, experiment, actual_times_array):
    for i, exp in enumerate(experiment):
        predicted_time = model_coeffs[0] + model_coeffs[1] / ( 2**exp[1] ) + model_coeffs[2] * exp[1] + model_coeffs[3] * ( 2**exp[1] )
        print 'Predicted Time: ', str(predicted_time)
        print 'Actual Time: ', str(actual_times_array[i])
        print 'Percent Difference: ', str(abs(predicted_time - actual_times_array[i])/actual_times_array[i])

def get_cost(num_machines, inst_type):
    ret_dict = get_bid(inst_type, region)
    return ret_dict['bid']*num_machines

def design_experiments(budget, duration, training_budget, duration_budget, inst_types):

    inst_type_experiments = []
    for exp_count, inst_type in enumerate(inst_types):
        scales = [i for i in range(1,11)]
        machs = []
        i = 0
        while get_cost(2**i, inst_type) <= budget*training_budget:
            machs.append(i)
            i += 1

        if machs == []:
            print 'The instance type selected was too expensive to run an experiment on our budget.  Skipping.'
            continue

        exps = []
        for i in range(len(scales)):
            for j in range(len(machs)):
                exps.append([scales[i], machs[j]])
        print '\nExperiment', str(exp_count) + ':\t' + inst_type + '\n'
        optimal_experiments = A_optimal_experimental_design(exps, training_budget*budget, duration_budget*duration)
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

def integer_program():
    1

def main():
    glob.set_globals()
    init('s3://url-combined-data/url_combined', 3231961, 10, 1, .1, .2, 'spark_test/target/scala-2.11/ml-test_2.11-1.0.jar', 'logistic_regression')
    #coeffs = list(fit_model([[9, 0], [9, 2], [9, 4], [10, 1], [10, 3]], [26.0001, 28.0901, 30.0203, 26.1923, 28.1294])[0])
    #print coeffs

if __name__ == "__main__":
    main()
