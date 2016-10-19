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
inst_types = ['m3.2xlarge']#['m3.xlarge', 'm2.2xlarge', 'm3.2xlarge']

def init(s3url, num_features, budget, duration, training_budget, duration_budget, jar_path, algorithm, init_time_str):
    
    experiments = design_experiments(budget, duration, training_budget, duration_budget, inst_types)

    # Run Experiments from Optimal Experimental Design
    #ADD IN PAY MECHANISM SOME WAY
    master_ip, master_id = start_master_node(master_type)
    #master_ip = '128.111.179.169'

    all_model_coeffs = []
    #all_actual_times = []

    for i, experiment in enumerate(experiments):
        configure_experiment_machines(experiment, inst_types[i], master_ip, master_id, s3url)


        time_array = []
        #actual_times_array = []
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
            configure_and_run_experiment_frameworks('experiment', num_features, exp, sub_nodes_info, s3url, jar_path, algorithm, 100)

            metric = read_job_time(master_ip)
            #SOMEHOW GET METRICS
            print "METRIC: ", str(metric)
            if metric != None:
                time_array.append(metric)



        model_coeffs = list(fit_model(experiment, time_array)[0])
        all_model_coeffs.append(model_coeffs)
        #all_actual_times.append(actual_times_array)

        #predicted_vs_actual(model_coeffs, experiment, actual_times_array)

    print repr(all_model_coeffs)
    os.system('mkdir init_experiments/experiment' + init_time_str)
    f = open('init_experiments/experiment' + init_time_str + '/model_coeffs', 'w')
    f.write(repr(all_model_coeffs) + '\n')
    f.close()
    # f = open('init_experiments/experiment' + init_time_str + '/actual_times', 'w')
    # f.write(repr(all_actual_times) + '\n')
    # f.close()

def fixed_design_init(s3url, num_features, budget, duration, training_budget, duration_budget, jar_path, algorithm, init_time_str, exps_array):
    
    experiments = exps_array#kevin_experiment_methodology(budget, training_budget)
    #sys.exit('We out!')
    # Run Experiments from Optimal Experimental Design
    #ADD IN PAY MECHANISM SOME WAY
    master_ip, master_id = start_master_node(master_type)
    #master_ip = '128.111.179.169'

    #all_model_coeffs = []
    #all_actual_times = []

    configure_experiment_machines(experiments, inst_types[0], master_ip, master_id, s3url)


    time_array = []
    #actual_times_array = []
    #SPLIT DATA FOR EXP
    prev_exp = None
    second_prev = None
    prev_exp_percent = 0
    os.system('mkdir init_experiments/experiment' + init_time_str)
    os.system('echo ' + s3url + ' >> init_experiments/experiment' + init_time_str + '/experiment_times')
    for exp in experiments:
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
            configure_and_run_experiment_frameworks('experiment', num_features, exp, sub_nodes_info, s3url, jar_path, algorithm, exp[2], True, prev_exp_percent)
        else:
            configure_and_run_experiment_frameworks('experiment', num_features, exp, sub_nodes_info, s3url, jar_path, algorithm, exp[2], False, prev_exp_percent)


        print "\n\n#######################################################"
        print "#######################################################\n\n"
        print "@@END EXPERIMENT", repr(exp)
        print "\n\n#######################################################"
        print "#######################################################\n\n"

        metric = read_job_time(master_ip)
        #SOMEHOW GET METRICS
        print "METRIC: ", str(metric)
        if metric != None:
            time_array.append(metric)
        os.system('echo ' + repr([exp, metric]) + ' >> init_experiments/experiment' + init_time_str + '/experiment_times')
        second_prev = prev_exp
        prev_exp = exp


def check_actual_times(s3url, num_features, budget, duration, training_budget, duration_budget, jar_path, algorithm, machines_num, exp_path):

    log_machs = int(math.log(machines_num, 2))
    master_ip, master_id = start_master_node(master_type)
    #master_ip = '128.111.179.171'
    experiment = [[100,log_machs,100]]
    actual_times = []

    for inst in inst_types:
        configure_experiment_machines(experiment, inst, master_ip, master_id, s3url)
        for exp in experiment:
            inst_ips = check_instance_status('ips', 'all')
            nodes_info = get_all_hosts(master_ip, inst_ips)
            configure_and_run_experiment_frameworks('actual', num_features, exp, nodes_info, s3url, jar_path, algorithm, 100)
            metric = read_job_time(master_ip)
            actual_times.append(metric)

    print repr(actual_times)
    f = open(exp_path + '/actual_times' + '_' + str(machines_num), 'w')
    f.write(repr(actual_times) + '\n')
    f.close()

    coeffs = read_coeffs(exp_path)

    predicted_time = predict_with_model_fixed_design(coeffs, log_machs, 100)
    print '\n\nMACHINE: ', inst_types[0], '\n\n'
    print 'Predicted Time: ', str(predicted_time)
    print 'Actual Time: ', str(actual_times[i])
    print 'Percent Difference: ', str(100*abs(predicted_time - actual_times[0])/predicted_time) + '%'

def kevin_experiment_methodology(budget, training_budget):
    # i = 0
    # while get_cost(2**i, inst_types[0]) <= budget*training_budget:
    #     i += 1
    # max_machs = 2**(i-1)
    max_machs = 8

    # scale, iterations, machine count
    #test_size_ratios = [1]#[(2**i)*100 for i in range(-1,1)]
    #machine_nums = [i + 1 for i in range(int(math.log(max_machs, 2)))][-1:]
    #iterations = [i for i in range(1,3)]

    experiments = [[50,2,1] for i in range(5)] + [[50,2,2] for i in range(5)]
                #[[50,3,1] for i in range(30)] + [[12.5,1,2] for i in range(30)] + [[25,2,2] for i in range(30)] + [[50,3,2] for i in range(30)]
    # for i in range(30):
    #     for ratio in test_size_ratios:
    #         for machine_num in machine_nums:
    #             for iter_ in iterations:
    #                 experiments.append([ratio, machine_num, iter_])
    print len(experiments)
    print experiments
    return experiments



def read_coeffs(exp_path):
    f = open(exp_path + '/model_coeffs', 'r')
    coeffs_array = ast.literal_eval(f.readlines()[0].strip())
    return coeffs_array


def time_str():
    return strftime("-%Y-%m-%d-%H-%M-%S", gmtime())

def fit_model_fixed_design(experiment_array, time_array):
    exp_list = []
    for elem in experiment_array:
        exp_arr = [1, float(elem[0])/(2**elem[1]), elem[1], 2**elem[1], elem[2] - 1]
        exp_list.append(np.asarray(exp_arr))
    exp_mat = np.asarray(exp_list)
    nnls_coeffs = nnls(exp_mat, np.asarray(time_array))
    return nnls_coeffs

def fit_model(experiment_array, time_array):
    exp_list = []
    for elem in experiment_array:
        exp_arr = [1, float(elem[0])/(2**elem[1]), elem[1], 2**elem[1]]
        exp_list.append(np.asarray(exp_arr))
    exp_mat = np.asarray(exp_list)
    nnls_coeffs = nnls(exp_mat, np.asarray(time_array))
    return nnls_coeffs

def predict_with_model_fixed_design(coeffs, x, iterations):
    predicted_time = coeffs[0] + coeffs[1] / ( 2**x ) + coeffs[2] * x + coeffs[3] * ( 2**x ) + coeffs[4]*(iterations - 1)
    return predicted_time

def predict_with_model(coeffs, x):
    predicted_time = coeffs[0] + coeffs[1] / ( 2**x ) + coeffs[2] * x + coeffs[3] * ( 2**x )
    return predicted_time



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

def integer_program():
    1

def polyfit_func(exps, times, deg):
    exps = map(lambda x: x[1:-1], exps)
    print repr(dir(multipolyfit))
    coeffs, degrees = list(multipolyfit.multipolyfit(exps, times, deg, powers_out=True))
    degrees = map(lambda x: list(x), degrees)
    return coeffs, degrees


def predict_poly_model(x1, x2, degrees):
    total = 0
    for degree in degrees:
        total += (x1**degree[0])*(x2**degree[1])
    return total



def main():
    glob.set_globals()



    for i in range(1,2):
        init_time_str = time_str()
        exps_array = [[12.5,1,30] for i in range(29)]
        #init('s3://url-combined-data/url_combined', 3231961, 10, 1, .1, .2, 'spark_test/target/scala-2.11/ml-test_2.11-1.0.jar', 'logistic_regression')
        fixed_design_init('s3://higgs-data/higgs', 28, 20, 1, .1, .2, 'spark_test/target/scala-2.11/ml-test_2.11-1.0.jar', 'logistic_regression', init_time_str + '_2_machine_30_epochs_30_trials', exps_array)
        # coeffs = list(fit_model([[9, 0], [9, 2], [9, 4], [10, 1], [10, 3]], [26.0001, 28.0901, 30.0203, 26.1923, 28.1294])[0])
        #print coeffs

    for i in range(1,2):
        init_time_str = time_str()
        exps_array = [[6.25,0,30] for i in range(30)]
        #init('s3://url-combined-data/url_combined', 3231961, 10, 1, .1, .2, 'spark_test/target/scala-2.11/ml-test_2.11-1.0.jar', 'logistic_regression')
        fixed_design_init('s3://higgs-data/higgs', 28, 20, 1, .1, .2, 'spark_test/target/scala-2.11/ml-test_2.11-1.0.jar', 'logistic_regression', init_time_str + '_1_machine_30_epochs_30_trials', exps_array)
        # coeffs = list(fit_model([[9, 0], [9, 2], [9, 4], [10, 1], [10, 3]], [26.0001, 28.0901, 30.0203, 26.1923, 28.1294])[0])
        #print coeffs

    for i in range(1,2):
        init_time_str = time_str()
        exps_array = [[100,4,500] for i in range(3)]
        #init('s3://url-combined-data/url_combined', 3231961, 10, 1, .1, .2, 'spark_test/target/scala-2.11/ml-test_2.11-1.0.jar', 'logistic_regression')
        fixed_design_init('s3://higgs-data/higgs', 28, 20, 1, .1, .2, 'spark_test/target/scala-2.11/ml-test_2.11-1.0.jar', 'logistic_regression', init_time_str + '_16_machine_30_epochs_2_trials', exps_array)
        # coeffs = list(fit_model([[9, 0], [9, 2], [9, 4], [10, 1], [10, 3]], [26.0001, 28.0901, 30.0203, 26.1923, 28.1294])[0])
        #print coeffs

    for i in range(1,2):
        init_time_str = time_str()
        exps_array = [[12.5,1,1] for i in range(30)]
        #init('s3://url-combined-data/url_combined', 3231961, 10, 1, .1, .2, 'spark_test/target/scala-2.11/ml-test_2.11-1.0.jar', 'logistic_regression')
        fixed_design_init('s3://higgs-data/higgs', 28, 20, 1, .1, .2, 'spark_test/target/scala-2.11/ml-test_2.11-1.0.jar', 'logistic_regression', init_time_str + '_2_machine_1_epochs_30_trials', exps_array)
        # coeffs = list(fit_model([[9, 0], [9, 2], [9, 4], [10, 1], [10, 3]], [26.0001, 28.0901, 30.0203, 26.1923, 28.1294])[0])
        #print coeffs

    for i in range(1,2):
        init_time_str = time_str()
        exps_array = [[6.25,0,1] for i in range(30)]
        #init('s3://url-combined-data/url_combined', 3231961, 10, 1, .1, .2, 'spark_test/target/scala-2.11/ml-test_2.11-1.0.jar', 'logistic_regression')
        fixed_design_init('s3://higgs-data/higgs', 28, 20, 1, .1, .2, 'spark_test/target/scala-2.11/ml-test_2.11-1.0.jar', 'logistic_regression', init_time_str + '_1_machine_1_epochs_30_trials', exps_array)
        # coeffs = list(fit_model([[9, 0], [9, 2], [9, 4], [10, 1], [10, 3]], [26.0001, 28.0901, 30.0203, 26.1923, 28.1294])[0])
        #print coeffs


    #os.system('s3cmd -c euca00/euca00-s3cfg put generated_higgs_0 s3://higgs-data/generated_higgs_0')

    # os.system('s3cmd -c euca00/euca00-s3cfg put generated_low_info_higgs_0 s3://higgs-data/generated_low_info_higgs_0')


    # for i in range(1,2):
    #     init_time_str = time_str()
    #     exps_array = [[12.5,1,1] for i in range(5)] + [[12.5,1,6] for i in range(5)]
    #     #init('s3://url-combined-data/url_combined', 3231961, 10, 1, .1, .2, 'spark_test/target/scala-2.11/ml-test_2.11-1.0.jar', 'logistic_regression')
    #     fixed_design_init('s3://higgs-data/generated_higgs', 28, 20, 1, .1, .2, 'spark_test/target/scala-2.11/ml-test_2.11-1.0.jar', 'logistic_regression', init_time_str, exps_array)
    #     # coeffs = list(fit_model([[9, 0], [9, 2], [9, 4], [10, 1], [10, 3]], [26.0001, 28.0901, 30.0203, 26.1923, 28.1294])[0])
    #     #print coeffs

    # for i in range(1,2):
    #     init_time_str = time_str()
    #     exps_array = [[25,2,1] for i in range(5)] + [[25,2,6] for i in range(5)]
    #     #init('s3://url-combined-data/url_combined', 3231961, 10, 1, .1, .2, 'spark_test/target/scala-2.11/ml-test_2.11-1.0.jar', 'logistic_regression')
    #     fixed_design_init('s3://higgs-data/generated_higgs', 28, 20, 1, .1, .2, 'spark_test/target/scala-2.11/ml-test_2.11-1.0.jar', 'logistic_regression', init_time_str, exps_array)
    #     # coeffs = list(fit_model([[9, 0], [9, 2], [9, 4], [10, 1], [10, 3]], [26.0001, 28.0901, 30.0203, 26.1923, 28.1294])[0])
    #     #print coeffs

    # for i in range(1,2):
    #     init_time_str = time_str()
    #     exps_array = [[50,3,1] for i in range(5)] + [[50,3,6] for i in range(5)]
    #     #init('s3://url-combined-data/url_combined', 3231961, 10, 1, .1, .2, 'spark_test/target/scala-2.11/ml-test_2.11-1.0.jar', 'logistic_regression')
    #     fixed_design_init('s3://higgs-data/generated_higgs', 28, 20, 1, .1, .2, 'spark_test/target/scala-2.11/ml-test_2.11-1.0.jar', 'logistic_regression', init_time_str, exps_array)
    #     # coeffs = list(fit_model([[9, 0], [9, 2], [9, 4], [10, 1], [10, 3]], [26.0001, 28.0901, 30.0203, 26.1923, 28.1294])[0])
    #     #print coeffs


    # for i in range(1,2):
    #     init_time_str = time_str()
    #     exps_array = [[100,3,100] for i in range(1)]
    #     #init('s3://url-combined-data/url_combined', 3231961, 10, 1, .1, .2, 'spark_test/target/scala-2.11/ml-test_2.11-1.0.jar', 'logistic_regression')
    #     fixed_design_init('s3://higgs-data/generated_higgs', 28, 20, 1, .1, .2, 'spark_test/target/scala-2.11/ml-test_2.11-1.0.jar', 'logistic_regression', init_time_str, exps_array)
    #     # coeffs = list(fit_model([[9, 0], [9, 2], [9, 4], [10, 1], [10, 3]], [26.0001, 28.0901, 30.0203, 26.1923, 28.1294])[0])
    #     #print coeffs

    # for i in range(1,2):
    #     init_time_str = time_str()
    #     exps_array = [[12.5,1,1] for i in range(5)] + [[12.5,1,6] for i in range(5)]
    #     #init('s3://url-combined-data/url_combined', 3231961, 10, 1, .1, .2, 'spark_test/target/scala-2.11/ml-test_2.11-1.0.jar', 'logistic_regression')
    #     fixed_design_init('s3://higgs-data/generated_low_info_higgs', 28, 20, 1, .1, .2, 'spark_test/target/scala-2.11/ml-test_2.11-1.0.jar', 'logistic_regression', init_time_str, exps_array)
    #     # coeffs = list(fit_model([[9, 0], [9, 2], [9, 4], [10, 1], [10, 3]], [26.0001, 28.0901, 30.0203, 26.1923, 28.1294])[0])
    #     #print coeffs

    # for i in range(1,2):
    #     init_time_str = time_str()
    #     exps_array = [[25,2,1] for i in range(5)] + [[25,2,6] for i in range(5)]
    #     #init('s3://url-combined-data/url_combined', 3231961, 10, 1, .1, .2, 'spark_test/target/scala-2.11/ml-test_2.11-1.0.jar', 'logistic_regression')
    #     fixed_design_init('s3://higgs-data/generated_low_info_higgs', 28, 20, 1, .1, .2, 'spark_test/target/scala-2.11/ml-test_2.11-1.0.jar', 'logistic_regression', init_time_str, exps_array)
    #     # coeffs = list(fit_model([[9, 0], [9, 2], [9, 4], [10, 1], [10, 3]], [26.0001, 28.0901, 30.0203, 26.1923, 28.1294])[0])
    #     #print coeffs

    # for i in range(1,2):
    #     init_time_str = time_str()
    #     exps_array = [[50,3,1] for i in range(5)] + [[50,3,6] for i in range(5)]
    #     #init('s3://url-combined-data/url_combined', 3231961, 10, 1, .1, .2, 'spark_test/target/scala-2.11/ml-test_2.11-1.0.jar', 'logistic_regression')
    #     fixed_design_init('s3://higgs-data/generated_low_info_higgs', 28, 20, 1, .1, .2, 'spark_test/target/scala-2.11/ml-test_2.11-1.0.jar', 'logistic_regression', init_time_str, exps_array)
    #     # coeffs = list(fit_model([[9, 0], [9, 2], [9, 4], [10, 1], [10, 3]], [26.0001, 28.0901, 30.0203, 26.1923, 28.1294])[0])
    #     #print coeffs

    # for i in range(1,2):
    #     init_time_str = time_str()
    #     exps_array = [[100,4,1] for i in range(5)] + [[100,4,6] for i in range(5)]
    #     #init('s3://url-combined-data/url_combined', 3231961, 10, 1, .1, .2, 'spark_test/target/scala-2.11/ml-test_2.11-1.0.jar', 'logistic_regression')
    #     fixed_design_init('s3://higgs-data/higgs', 28, 20, 1, .1, .2, 'spark_test/target/scala-2.11/ml-test_2.11-1.0.jar', 'logistic_regression', init_time_str, exps_array)
    #     # coeffs = list(fit_model([[9, 0], [9, 2], [9, 4], [10, 1], [10, 3]], [26.0001, 28.0901, 30.0203, 26.1923, 28.1294])[0])
    #     #print coeffs


    # for i in range(1,2):
    #     init_time_str = time_str()
    #     exps_array = [[100,4,100] for i in range(2)]
    #     #init('s3://url-combined-data/url_combined', 3231961, 10, 1, .1, .2, 'spark_test/target/scala-2.11/ml-test_2.11-1.0.jar', 'logistic_regression')
    #     fixed_design_init('s3://higgs-data/higgs', 28, 20, 1, .1, .2, 'spark_test/target/scala-2.11/ml-test_2.11-1.0.jar', 'logistic_regression', init_time_str, exps_array)
    #     # coeffs = list(fit_model([[9, 0], [9, 2], [9, 4], [10, 1], [10, 3]], [26.0001, 28.0901, 30.0203, 26.1923, 28.1294])[0])
    #     #print coeffs












    # for i in range(1,2):
    #     init_time_str = time_str()
    #     exps_array = [[100,3,100] for i in range(1)]
    #     #init('s3://url-combined-data/url_combined', 3231961, 10, 1, .1, .2, 'spark_test/target/scala-2.11/ml-test_2.11-1.0.jar', 'logistic_regression')
    #     fixed_design_init('s3://higgs-data/generated_low_info_higgs', 28, 20, 1, .1, .2, 'spark_test/target/scala-2.11/ml-test_2.11-1.0.jar', 'logistic_regression', init_time_str, exps_array)
    #     # coeffs = list(fit_model([[9, 0], [9, 2], [9, 4], [10, 1], [10, 3]], [26.0001, 28.0901, 30.0203, 26.1923, 28.1294])[0])
    #     #print coeffs

    # check_actual_times('s3://higgs-data/higgs', 28, 20, 1, .1, .2, 'spark_test/target/scala-2.11/ml-test_2.11-1.0.jar', 'logistic_regression', 8, 'init_experiments/experiment' + init_time_str)
    # for inst in inst_types:
    #     cost = get_cost(8, inst)
    #     print 'Cost of', inst, 'for at least 1 hour:', str(cost)


    # experiments_array = [[100, 3, 1], [100, 3, 2], [100, 2, 2], [100, 2, 1], [100, 1, 2], [100, 1, 1]]
    # time_array = [229.16, 285.01, 604.95, 497.08, 1264.14, 1012.84]

    # # experiments_array = [[100, 3, 1], [100, 3, 2], [100, 2, 2], [100, 2, 1]]
    # # time_array = [229.16, 285.01, 604.95, 497.08]

    # # coeffs, r2 = fit_model_fixed_design(experiments_array, time_array)
    # coeffs, degrees = polyfit_func(experiments_array, time_array, 3)
    # # # print coeffs, degrees
    # # # print experiments_array
    # # # print time_array
    # out1 = predict_poly_model(4, 1, degrees)
    # out2 = predict_poly_model(4, 2, degrees)
    # out3 = out1 + (out2 - out1)*59
    # # # print coeffs, r2
    # # # out1 = predict_with_model_fixed_design(model_coeffs, 4, 1)
    # # # out2 = predict_with_model_fixed_design(model_coeffs, 4, 2)
    # # # out3 = predict_with_model_fixed_design(model_coeffs, 4, 100)
    # print out1, out2, out3



if __name__ == "__main__":
    main()
