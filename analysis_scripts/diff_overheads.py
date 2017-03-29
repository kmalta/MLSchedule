import os
import ast
from dateutil import parser
import matplotlib.pyplot as plt
import numpy as np
import math


def get_directories(root):
    try:
        for directory in os.walk(root):
            directories = directory[1]
            break
    except:
        directories = ''
    return directories

def find_time(line):
    return line.split('/')[2].split()[1]

def get_overheads(dir_, exp_dir, idx):
    dir_path = exp_dir + '/' + dir_
    f = open(dir_path + '/all_iters', 'r')
    all_iters = float(f.readlines()[idx])
    f.close()

    f = open(dir_path + '/epochs', 'r')
    epochs = ast.literal_eval(f.readlines()[idx])
    f.close()

    return all_iters, epochs

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

def get_init(dir_, exp_dir, idx):
    dir_path = exp_dir + '/' + dir_
    f = open(dir_path + '/logs/log_trial_' + str(idx), 'r')
    log = f.readlines()
    f.close()

    flag1 = 0
    flag2 = 0
    time1 = 0
    time2 = 0
    diff = []
    first_time1 = 0
    first_time2 = 0

    for i, line in enumerate(log):
        if 'SparkContext: Running Spark' in line:
            first_time1 = find_time(line)

        if (': Job ' in line and int(line.split(': Job ')[1].split()[0]) == 2) or flag1 == 1:
            if flag1 == 0:
                flag1 = 1
                continue
            if 'SparkContext: Starting job: ' in line:
                if '[GC' in line:
                    first_time2 = find_time(line)
                else:
                    first_time2 = find_time(line)
                seconds = (parser.parse(first_time2) - parser.parse(first_time1)).total_seconds()
                #print "INITIAL TIME:", str(seconds)
                return first_time1, seconds
                break
            else:
                continue

def get_setup(dir_, exp_dir, idx):
    dir_path = exp_dir + '/' + dir_
    f = open(dir_path + '/logs/log_trial_' + str(idx), 'r')
    log = f.readlines()
    f.close()

    first_time = 0
    second_time = 0

    for i, line in enumerate(log):
        if 'SparkContext: Running Spark' in line:
            first_time = find_time(line)
        if 'SparkContext: Starting job: ' in line:
            second_time = find_time(line)
            seconds = (parser.parse(second_time) - parser.parse(first_time)).total_seconds()
            return seconds


def get_last(dir_, exp_dir, idx):
    dir_path = exp_dir + '/' + dir_
    f = open(dir_path + '/logs/log_trial_' + str(idx), 'r')
    log = f.readlines()
    f.close()

    for i, line in enumerate(log):
        if 'ShutdownHookManager: Deleting directory /tmp' in line:
            return find_time(line)

def get_jobs(dir_, exp_dir, idx):
    dir_path = exp_dir + '/' + dir_
    f = open(dir_path + '/logs/log_trial_' + str(idx), 'r')
    log = f.readlines()
    f.close()

    job_0 = 0
    job_1 = 0
    job_2 = 0
    diff = []
    first_time = ''
    second_time = ''
    flag = -1

    for i, line in enumerate(log):

        if (': Job ' in line and int(line.split(': Job ')[1].split()[0]) == 0):
            job_0 = float(line.split()[-2])
            first_time = find_time(line)
            flag = 0
        if (': Job ' in line and int(line.split(': Job ')[1].split()[0]) == 1):
            job_1 = float(line.split()[-2])
            first_time = find_time(line)
            flag = 1
        if (': Job ' in line and int(line.split(': Job ')[1].split()[0]) == 2):
            job_2 = float(line.split()[-2])
            first_time = find_time(line)
            flag = 2
        if 'SparkContext: Starting job: ' in line and flag >= 0:
            second_time = find_time(line)

            diff.append((parser.parse(second_time) - parser.parse(first_time)).total_seconds())
            if flag == 2:
                return job_0, job_1, job_2, diff[0], diff[1], diff[2]

def get_last_diff(dir_, exp_dir, idx):
    dir_path = exp_dir + '/' + dir_
    f = open(dir_path + '/logs/log_trial_' + str(idx), 'r')
    log = f.readlines()
    f.close()

    first_time = ''
    second_time = ''

    for i, line in enumerate(log):
        if 'Job ' in line and ' finished: ' in line:
            first_time = find_time(line)
        if 'ShutdownHookManager: Deleting directory' in line:
            second_time = find_time(line)
            return (parser.parse(second_time) - parser.parse(first_time)).total_seconds()

def print_all_times(dir_, all_times):

    print "###########################################################"
    print "###########################################################"
    print
    print "DATASET:", dir_.split('_')[1]
    print
    print "Init:", repr(all_times[0])
    print "Job 0:", repr(all_times[1])
    print "Diff 0:", repr(all_times[4])
    print "Job 1:", repr(all_times[2])
    print "Diff 1:", repr(all_times[5])
    print "Job 2:", repr(all_times[3])
    print "Diff 2:", repr(all_times[6])
    print "Epoch Sum:", repr(all_times[7])
    print "Differences", repr(all_times[8])
    print "Last_diff", repr(all_times[9])
    print
    print "###########################################################"
    print "###########################################################"
    print
    print
    print


def get_all_metrics(epoch_dir, exp_dir, show, idx):

    all_time, epochs = get_overheads(epoch_dir, exp_dir, idx)
    init = get_setup(epoch_dir, exp_dir, idx)
    diffs = get_diff(epoch_dir, exp_dir, idx)
    last_time = get_last(epoch_dir, exp_dir, idx)
    job_0, job_1, job_2, diff_0, diff_1, diff_2 = get_jobs(epoch_dir, exp_dir, idx)
    last_diff = get_last_diff(epoch_dir, exp_dir, idx)

    all_times = [init, job_0, job_1, job_2, diff_0, diff_1, diff_2, sum(epochs), sum(diffs), last_diff, epochs, diffs]

    if show == True:
        print all_times(all_times)
    return all_time, all_times

def add_column(mat, arr):
    if mat == []:
        mat = [[] for j in range(len(arr[1]))]
    for i in range(len(arr[1])):
        mat[i].append(arr[1][i])
    return mat

#all_time, init, job_0, job_1, job_2, diff_0, diff_1, diff_2, epochs_sum, diffs_sum, last_diff, epochs, diffs
def get_metrics_across_machine_counts(root):
    directories = get_directories(root)
    all_times = []
    for dir_ in directories:
        all_times = add_column(all_times, get_all_metrics(dir_, root, False))
        dir_arr = dir_.split('_')
        if dir_arr[-8] == '16':
            print_all_times(dir_, all_times)
            all_times = []
        elif dir_arr[-8] == '8' and dir_arr[-9] in ['kdda', 'kddb']:
            print_all_times(dir_, all_times)
            all_times = []

def main():
    get_metrics_across_machine_counts('exps_1_17_17_all_data')


if __name__ == "__main__":
    main()



