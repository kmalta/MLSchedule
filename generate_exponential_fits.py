import sys
import ast
import math
import numpy as np

def fit_exp(epoch_times, x_vals):
    log_epoch_times = map(lambda x: math.log(x), epoch_times)
    m,b = np.polyfit(x_vals, log_epoch_times, 1)
    A = math.exp(b)
    B = m
    return A, B

def predict_exp(x_val, A, B):
    return A*math.exp(B*x_val)

def predict_time(num_epochs_observed, total_num_epochs, all_ys):

    y_vals = all_ys[:num_epochs_observed]
    x_vals = [i + 1 for i in range(len(y_vals))]

    A, B = fit_exp(y_vals, x_vals)

    num_to_predict = total_num_epochs - num_epochs_observed
    xs_to_predict = [i + num_epochs_observed for i in range(num_to_predict)]
    ys_predicted = [predict_exp(x_val, A, B) for x_val in xs_to_predict]


    combine_predict_and_real_ys = y_vals + ys_predicted

    print combine_predict_and_real_ys

    sum_all_ys = sum(all_ys)
    sum_combine_predict_and_real_ys =  sum(combine_predict_and_real_ys)
    percent_err = math.fabs(sum_all_ys - sum_combine_predict_and_real_ys)/sum_all_ys *100

    print "\n"
    print "The real sum is:", str(sum_all_ys)
    print "For", str(len(x_vals)), "observed epochs, the predicted sum is:", str(sum_combine_predict_and_real_ys)
    print "The percent error is:", str(percent_err)


def predict_time_modified_tail_behavior(num_epochs_observed, num_to_predict, total_num_epochs, all_ys):

    y_vals = all_ys[:num_epochs_observed]
    x_vals = [i + 1 for i in range(len(y_vals))]

    if num_to_predict != 0:

        A, B = fit_exp(y_vals, x_vals)

        xs_to_predict = [i + num_epochs_observed for i in range(num_to_predict)]
        ys_predicted = [predict_exp(x_val, A, B) for x_val in xs_to_predict]


        combine_predict_and_real_ys = y_vals + ys_predicted + [ys_predicted[-1] for i in range(total_num_epochs - num_epochs_observed - num_to_predict)]
    else:
        combine_predict_and_real_ys = y_vals + [y_vals[-1] for i in range(total_num_epochs - num_epochs_observed)]

    sum_all_ys = sum(all_ys)
    sum_combine_predict_and_real_ys =  sum(combine_predict_and_real_ys)
    percent_err = math.fabs(sum_all_ys - sum_combine_predict_and_real_ys)/sum_all_ys *100

    print "\n"
    print "The real sum is:", str(sum_all_ys)
    print "For", str(len(x_vals)), "observed epochs, and ", str(num_to_predict), "predicted epochs, the predicted sum is:", str(sum_combine_predict_and_real_ys)
    print "The percent error is:", str(percent_err)


def predicted_suite_of_exps(epochs_observed, num_epochs_predicted, total_epochs, epoch_data):

    print "\n\nSTART!!!\n\n\n"
    for i in epochs_observed:
        for j in num_epochs_predicted:
            predict_time_modified_tail_behavior(i, j, 500, epoch_data)
    print "\n\n\nEND!!!\n\n"


def predict_end_behavior(paths):



path1 = 'aristotle_all_in_one_exps/' + 'experiment-2016-10-29-16-36-02_kddb_1_machine_500_epochs_30_trials'
path2 = 'aristotle_all_in_one_exps/' + 'experiment-2016-10-29-16-36-02_kddb_1_machine_500_epochs_30_trials'
path3 = 'aristotle_all_in_one_exps/' + 'experiment-2016-10-29-16-36-02_kddb_1_machine_500_epochs_30_trials'

paths = [path1, path2, path3]
epochs = 500

f = open(path + '/epochs', 'r')
#f2 = open(path + '/epoch_averages', 'r')



runs = f.readlines()
first_run = ast.literal_eval(runs[0])
second_run = ast.literal_eval(runs[1])
third_run = ast.literal_eval(runs[2])

predicted_epochs = [0,1,2,3,4,5,10]

predicted_suite_of_exps([30], predicted_epochs, epochs, epoch_averages)
predicted_suite_of_exps([30], predicted_epochs, epochs, first_run)
predicted_suite_of_exps([30], predicted_epochs, epochs, second_run)
predicted_suite_of_exps([30], predicted_epochs, epochs, third_run)



