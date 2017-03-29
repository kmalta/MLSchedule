import sys
import ast
from matplotlib import pyplot as plt
import numpy as np
import math
import pandas as pd
import random
import os
from dateutil import parser



def get_directories(root):
    try:
        for directory in os.walk(root):
            directories = directory[1]
            break
    except:
        directories = ''
    return directories

def check_args(dataset, machines, epochs, dist_type):
    if dataset not in ['higgs', 'susy', 'url', 'kdda', 'kddb']:
        sys.exit("Not a valid dataset.")
    if machines not in ['1', '2', '4']:
        sys.exit("Not a valid dataset.")
    if epochs not in ['30', '50', '500']:
        sys.exit("Not a valid numer of epochs.")
    if dist_type not in ['linear', 'exp']:
        sys.exit("Not a valid distance function type.")

def filter_dirs(dirs, dataset, machines, epochs):
    filtered = filter(lambda x: dataset in x, dirs)
    filtered = filter(lambda x: machines + '_machine' in x, filtered)
    filtered = filter(lambda x: epochs + '_epochs' in x, filtered)
    return filtered[0]

def get_synth_coordinates(dataset):
    synth_coordinates = []

    f = open('synth_matching/' + dataset + '/coordinates', 'r')
    data = f.readlines()
    f.close()

    coordinates = ast.literal_eval(data[0])

    for line in data[1:]:
        synth_coordinates.append(ast.literal_eval(line))

    return coordinates, synth_coordinates

def get_synth_dirs(machines, synth_coordinates, dirs):
    dir_tuples = []
    for coord in synth_coordinates:
        filtered = filter(lambda x: machines + '_machine' in x, dirs)
        filtered = filter(lambda x: str(coord[0]) + '_' + str(coord[1]) in x, filtered)
        dir_tuples.append([coord, filtered[0]])
    return dir_tuples


def get_synth_epochs(dir_tuples):
    closest_synths = []
    for dir_tuple in dir_tuples:
        f = open('synth_exps_100k_1_12_17/' + dir_tuple[1] + '/epochs', 'r')
        synth_epochs = ast.literal_eval(f.readlines()[0])
        f.close()
        closest_synths.append([dir_tuple[0],synth_epochs])
    return closest_synths

def get_synth_diffs(dir_tuples):
    closest_synths = []
    for dir_tuple in dir_tuples:
        synth_diffs = get_diff(dir_tuple[1], 'synth_exps_100k_1_12_17', 0)
        closest_synths.append([dir_tuple[0],synth_diffs])
    return closest_synths

def find_time(line):
    return line.split('/')[2].split()[1]

def get_diff(dir_, root_dir, idx):
    dir_path = root_dir + '/' + dir_
    f = open(dir_path + '/logs/log_trial_' + str(idx), 'r')
    log = f.readlines()
    f.close()

    flag2 = 0
    time1 = 0
    time2 = 0
    diff = []
    first_time1 = 0
    first_time2 = 0

    for i, line in enumerate(log):

        if ': Job ' in line and int(line.split(': Job ')[1].split()[0]) > 2:
            flag2 = 1
            time1 = find_time(line)
        elif 'Starting job: ' in line and flag2 == 1:
            flag2 = 0
            time2 = find_time(line)
            diff.append((parser.parse(time2) - parser.parse(time1)).total_seconds())


        if flag2 == 0:
            continue
    return diff


def exp_weighted_dist(c1,c2,c3,c4,interior):
    d1 = np.linalg.norm(np.asarray(c1) - interior)
    d2 = np.linalg.norm(np.asarray(c2) - interior)
    d3 = np.linalg.norm(np.asarray(c3) - interior)
    d4 = np.linalg.norm(np.asarray(c4) - interior)
    d = [d1, d2, d3, d4]
    w = [np.exp(1/float(elem)) for elem in d]
    return [elem/sum(w) for elem in w]

def linearly_weighted_dist(c1,c2,c3,c4,interior):
    d1 = np.linalg.norm(np.asarray(c1) - interior)
    d2 = np.linalg.norm(np.asarray(c2) - interior)
    d3 = np.linalg.norm(np.asarray(c3) - interior)
    d4 = np.linalg.norm(np.asarray(c4) - interior)
    d = [d1, d2, d3, d4]
    w = [1/float(elem) for elem in d]
    return [elem/sum(w) for elem in w]


def synth_match(actual, closest_synths, coordinates, dist_type, plot):

    num_iters = len(closest_synths[0][1])
    if dist_type == 'linear':
        weights = linearly_weighted_dist(closest_synths[0][0], closest_synths[1][0], closest_synths[2][0], closest_synths[3][0], coordinates)
    elif dist_type == 'exp':
        weights = exp_weighted_dist(closest_synths[0][0], closest_synths[1][0], closest_synths[2][0], closest_synths[3][0], coordinates)
    else:
        sys.exit('That is not a valid distance function buddy.')

    combined_list = []
    for i in range(num_iters):
        value = 0.0
        for j in range(4):
            value += closest_synths[j][1][i]*weights[j]
        combined_list.append(value)

    observed = actual[30:50]
    closest_compared = combined_list[30:50]
    observed_dataseries = pd.Series(observed)
    closest_compared_dataseries = pd.Series(closest_compared)
    smoothed_observed = observed_dataseries.rolling(window=5, center=False).median()
    smoothed_closest_compared = closest_compared_dataseries.rolling(window=5, center=False).median()

    diffs = [x - y for x, y in zip(smoothed_closest_compared, smoothed_observed)]
    diffs = filter(lambda x:  math.isnan(x) == False, diffs)
    mean_diff = np.mean(diffs)
    new_list = actual[:50] + [combined_list[i] - mean_diff for i in range(50, num_iters)]


    if plot == True:
        print sum(new_list)
        print sum(actual)
        print math.fabs(sum(new_list) - sum(actual))/sum(actual) * 100
        plt.plot(actual, 'black')
        plt.plot(new_list, 'red')
        plt.show()

    return sum(new_list), sum(actual), math.fabs(sum(new_list) - sum(actual))/sum(actual) * 100

def get_averages(dist_type, coordinates, exp, closest_synths_epochs, closest_synths_diffs):
    f = open('exps_1_16_17/' + exp + '/epochs', 'r')
    all_epochs_500 = map(lambda x: ast.literal_eval(x), f.readlines())
    f.close()

    all_diffs_500 = []
    for i in range(len(all_epochs_500)):
        all_diffs_500.append(get_diff(exp, 'exps_1_16_17', i))

    predictions = []
    actuals = []
    perc_errors = []
    for epochs_500 in all_epochs_500:
        prediction, actual, perc_error = synth_match(epochs_500, closest_synths_epochs, coordinates, dist_type, False)
        predictions.append(prediction)
        actuals.append(actual)
        perc_errors.append(perc_error)
    print np.mean(predictions)
    print np.mean(actuals)
    print np.mean(perc_errors)
    print math.sqrt(np.var(predictions))
    print math.sqrt(np.var(actuals))
    print math.sqrt(np.var(perc_errors))
    print

    predictions = []
    actuals = []
    perc_errors = []
    for diff_500 in all_diffs_500:
        prediction, actual, perc_error = synth_match(diff_500, closest_synths_diffs, coordinates, dist_type, False)
        predictions.append(prediction)
        actuals.append(actual)
        perc_errors.append(perc_error)
    print np.mean(predictions)
    print np.mean(actuals)
    print np.mean(perc_errors)
    print math.sqrt(np.var(predictions))
    print math.sqrt(np.var(actuals))
    print math.sqrt(np.var(perc_errors))

def main():

    averages = 'no'

    clp = sys.argv
    try:
        dataset = clp[1]
        machines = clp[2]
        epochs = clp[3]
        dist_type = clp[4]
    except:
        1

    check_args(dataset, machines, epochs, dist_type)
    dirs = get_directories('exps_1_16_17')
    exp = filter_dirs(dirs, dataset, machines, epochs)


    coordinates, synth_coordinates = get_synth_coordinates(dataset)


    synth_dirs = get_directories('synth_exps_100k_1_12_17')

    dir_tuples = get_synth_dirs(machines, synth_coordinates, synth_dirs)

    closest_synths_epochs = get_synth_epochs(dir_tuples)

    #DEBUG
    # names = map(lambda x: x[0], closest_synths_epochs)
    # epos = map(lambda x: x[1], closest_synths_epochs)

    # print repr(names)
    # color_array = ['blue', 'red', 'purple', 'black']
    # for i in range(len(epos)):
    #     plt.plot(epos[i], color_array[i])

    # plt.show()

    closest_synths_diffs = get_synth_diffs(dir_tuples)

    if averages != 'no':
        get_averages(dist_type, coordinates, exp, closest_synths_epochs, closest_synths_diffs)
    else:
        f = open('exps_1_16_17/' + exp + '/epochs', 'r')
        epochs_500 = ast.literal_eval(f.readlines()[0])
        f.close()
        synth_match(epochs_500, closest_synths_epochs, coordinates, dist_type, True)
        diff_500 = get_diff(exp, 'exps_1_16_17', 0)
        synth_match(diff_500, closest_synths_diffs, coordinates, dist_type, True)

if __name__ == "__main__":
    main()


