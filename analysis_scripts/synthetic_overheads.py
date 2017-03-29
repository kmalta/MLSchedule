import sys
import ast
from matplotlib import pyplot as plt
import numpy as np
import math
import pandas as pd
import random


def new_dist(c1,c2,c3,c4,interior):
    d1 = np.linalg.norm(np.asarray(c1) - interior)
    d2 = np.linalg.norm(np.asarray(c2) - interior)
    d3 = np.linalg.norm(np.asarray(c3) - interior)
    d4 = np.linalg.norm(np.asarray(c4) - interior)
    d = [d1, d2, d3, d4]
    w = [np.exp(1/elem) for elem in d]
    return [elem/sum(w) for elem in w]


clp = sys.argv
dataset = clp[2]

averages = clp[1]


af = ''
if dataset == 'susy':
    af = 'experiment-2016-11-05-22-09-45_susy_2_machines_500_epochs_30_trials'
elif dataset == 'kdda':
    af = 'experiment-2016-11-12-00-26-50_kdda_2_machines_500_epochs_30_trials'
elif dataset == 'url':
    af = 'experiment-2016-11-06-04-36-44_url_combined_2_machines_500_epochs_30_trials'
elif dataset == 'higgs':
    af = 'experiment-2016-11-05-18-27-16_higgs_2_machines_500_epochs_30_trials'
elif dataset == 'kddb':
    af = 'experiment-2016-11-08-03-17-42_kddb_2_machines_500_epochs_30_trials'
else:
    sys.exit("NOT A DATASET!")

actual_epochs_file = 'exps_11_5_16/' + af + '/epochs'
actual_total_time_file = 'exps_11_5_16/' + af + '/all_iters'

closest_synths = []

f = open('synth_matching/' + dataset + '/epochs', 'r')
epoch_file_read = f.readlines()
f.close()

for i in range(len(epoch_file_read)):
    arr = ast.literal_eval(epoch_file_read[i])
    closest_synths.append(arr)

f = open('synth_matching/' + dataset + '/all_iters', 'r')
total_time_read = f.readlines()
f.close()

closest_synths_totals = []

for i in range(len(epoch_file_read)):
    tot = float(epoch_file_read[i])
    closest_synths_totals.append(arr)

f = open('synth_matching/' + dataset + '/coordinates', 'r')
coordinates = ast.literal_eval(f.readlines()[0])
f.close()


if averages == 'no':
    f = open(actual_epochs_file, 'r')
    epos = f.readlines()
    f.close()

    rando = random.randint(0,len(epos) - 1)
    actual_epochs = ast.literal_eval(epos[rando])

    f = open(actual_total_time_file, 'r')
    total_time = f.readlines()[0]
    actual_overhead = float(total_time) - sum(ac)

    # plt.plot(closest_synths[0][1], 'blue')
    # plt.plot(closest_synths[1][1], 'green')
    # plt.plot(closest_synths[2][1], 'red')
    # plt.plot(closest_synths[3][1], 'black')


    weights = new_dist(closest_synths[0][0], closest_synths[1][0], closest_synths[2][0], closest_synths[3][0], coordinates)
    print repr(weights)

    combined_list = []
    for i in range(500):
        value = 0.0
        for j in range(4):
            value += closest_synths[j][1][i]*weights[j]
        combined_list.append(value)


    observed = actual_epochs[30:50]
    closest_compared = combined_list[30:50]


    observed_dataseries = pd.Series(observed)
    closest_compared_dataseries = pd.Series(closest_compared)

    smoothed_observed = observed_dataseries.rolling(window=5, center=False).median()
    smoothed_closest_compared = closest_compared_dataseries.rolling(window=5, center=False).median()

    diffs = [x - y for x, y in zip(smoothed_closest_compared, smoothed_observed)]

    diffs = filter(lambda x:  math.isnan(x) == False, diffs)
    mean_diff = np.mean(diffs)

    new_list = actual_epochs[:50] + [combined_list[i] - mean_diff for i in range(50, 500)]



    print sum(new_list)
    print sum(actual_epochs)
    print math.fabs(sum(new_list) - sum(actual_epochs))/sum(actual_epochs) * 100


    actual_overhead - 

    plt.plot(actual, 'black')
    plt.plot(new_list, 'red')
    plt.show()


else:

    f = open(actual_file, 'r')

    epos = f.readlines()
    actuals = []
    for line in epos:
        actual = ast.literal_eval(line)
        actuals.append(actual)



    weights = new_dist(closest_synths[0][0], closest_synths[1][0], closest_synths[2][0], closest_synths[3][0], coordinates)
    #print repr(weights)

    combined_list = []
    for i in range(500):
        value = 0.0
        for j in range(4):
            value += closest_synths[j][1][i]*weights[j]
        combined_list.append(value)


    closest_compared = combined_list[30:50]
    closest_compared_dataseries = pd.Series(closest_compared)
    smoothed_closest_compared = closest_compared_dataseries.rolling(window=5, center=False).median()


    new_list_list = []
    actual_list = []
    perc_error_list = []

    for actual in actuals:
        observed = actual[30:50]
        observed_dataseries = pd.Series(observed)
        smoothed_observed = observed_dataseries.rolling(window=5, center=False).median()




        diffs = [x - y for x, y in zip(smoothed_closest_compared, smoothed_observed)]

        diffs = filter(lambda x:  math.isnan(x) == False, diffs)
        mean_diff = np.mean(diffs)

        new_list = actual[:50] + [combined_list[i] - mean_diff for i in range(50, 500)]



        new_list_list.append(sum(new_list))
        actual_list.append(sum(actual))
        perc_error_list.append(math.fabs(sum(new_list) - sum(actual))/sum(actual) * 100)

    # print new_list_list
    # print actual_list
    # print perc_error_list

    # print np.mean(new_list_list)
    # print np.mean(actual_list)
    # print np.mean(perc_error_list)
    # print math.sqrt(np.var(perc_error_list))
    print max(perc_error_list)


