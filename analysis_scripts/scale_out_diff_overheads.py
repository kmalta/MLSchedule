import os
import ast
from dateutil import parser
import matplotlib.pyplot as plt
import numpy as np
import math
import pprint
from itertools import groupby
import sys

synth_list = [
              'synth_cluster_scaling_exps_2_9_17_memory_only_h1',
              'synth_ram_exp_.5gb_h1_2_13_17_30_trials', 
              'synth_ram_exp_1gb_h1_2_13_17_30_trials', 
              'synth_ram_exp_2gb_h1_2_13_17_30_trials', 
              'synth_ram_exp_4gb_h1_2_13_17_30_trials', 
              'synth_ram_exp_8gb_h1_2_13_17_30_trials', 
             ]


real_list = [
            #'exps_1_17_17_all_data',
            #'m1_min_2_7_17_all_data',
            'hi1_min_2_7_17_all_data',
           ]

machine_names = [
                 #'cg1.4xlarge',
                 #'m1.large',
                 'hi1.4xlarge',
                ]

machine_ram = [
                #4, 
                #11.5, 
                35]

dict_entries = [
                "Init",
                "Job_0",
                "Diff_0",
                "Job_1",
                "Diff_1",
                "Job_2",
                "Diff_2",
                "Epoch_Sum",
                "Differences",
                "Last_diff"
               ]

datasets = ['higgs', 'susy', 'url', 'kdda', 'kddb']
datasets_bytes = [7.4, 2.3, 2.1, 2.5, 4.8]
samples = [11000000,5000000,2396130,19264097,8407752]
features = [28,18,3231961,29890095,20216830]


filtered_dict_entries = [
                        # "Init",
                        # "Job_0",
                        # "Diff_0",
                        # "Job_1",
                        # "Diff_1",
                        # "Job_2",
                        # "Diff_2",
                        "Epoch_Sum",
                        "Differences",
                        # "Last_diff"
                        ]

def get_directories(root):
    directories = ''
    try:
        for directory in os.walk(root):
            directories = directory[1]
            break
    except:
        sys.exit("We were unable to read the directory")
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


def print_all_synth_times(dir_, all_times):
    name_split = dir_.split('_')

    print "###########################################################"
    print "###########################################################"
    print
    print "DATASET:", name_split[1] + ' ' + name_split[3] + ' ' + name_split[4]
    #print "MACHINE COUNT:", name_split[]
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

    for i in range(len(all_times)):
        if isinstance(all_times[i], list):
            for j in range(len(all_times[i])):
                all_times[i][j] = round(all_times[i][j], 5)
        else:
            all_times[i] = round(all_times[i], 5)
    return all_time, all_times

def get_internal_epoch_times(dir_, exp_dir, idx):
    dir_path = exp_dir + '/' + dir_
    f = open(dir_path + '/logs/log_trial_' + str(idx), 'r')
    log = f.readlines()
    f.close()

    flag = 0
    shuffles = []
    results = []
    epochs = []

    for i, line in enumerate(log):

        if ': Job ' in line and int(line.split(': Job ')[1].split()[0]) > 2:
            flag = 1

        if flag != 0:
            if 'ShuffleMapStage ' in line and 'finished' in line:
                shuffles.append(float(line.split()[-2]))
            if 'ResultStage ' in line and 'finished' in line:
                results.append(float(line.split()[-2]))
            if ': Job ' in line and 'finished' in line:
                epochs.append(float(line.split()[-2]))

    internal_epoch_diffs = [x - y - z for x,y,z in zip(epochs, shuffles, results)]
    return shuffles, results, internal_epoch_diffs

def get_all_metrics_modified(epoch_dir, exp_dir, show, idx):

    all_time, epochs = get_overheads(epoch_dir, exp_dir, idx)
    shuffles, results, internal_epoch_diffs = get_internal_epoch_times(epoch_dir, exp_dir, idx)
    init = get_setup(epoch_dir, exp_dir, idx)
    diffs = get_diff(epoch_dir, exp_dir, idx)
    last_time = get_last(epoch_dir, exp_dir, idx)
    job_0, job_1, job_2, diff_0, diff_1, diff_2 = get_jobs(epoch_dir, exp_dir, idx)
    last_diff = get_last_diff(epoch_dir, exp_dir, idx)

    all_times = [init, job_0, job_1, 
                 job_2, diff_0, diff_1, 
                 diff_2, sum(shuffles), sum(results), 
                 sum(internal_epoch_diffs), sum(epochs), 
                 sum(diffs), last_diff]

    if show == True:
        print all_times(all_times)

    for i in range(len(all_times)):
        if isinstance(all_times[i], list):
            for j in range(len(all_times[i])):
                all_times[i][j] = round(all_times[i][j], 5)
        else:
            all_times[i] = round(all_times[i], 5)
    return all_time, all_times, [shuffles, results, internal_epoch_diffs, epochs, diffs]

def get_task_counts(dir_, exp_dir):
    dir_path = exp_dir + '/' + dir_
    f = open(dir_path + '/logs/log_trial_' + str(0), 'r')
    log = f.readlines()
    f.close()

    flag = 0

    shuffle_stages = 0
    result_stages = 0

    for i, line in enumerate(log):

        if ': Job ' in line and int(line.split(': Job ')[1].split()[0]) > 2:
            flag = 1

        if ': Job ' in line and int(line.split(': Job ')[1].split()[0]) > 3:
            return shuffle_stages, result_stages

        if flag != 0:
            if 'Finished task ' in line:
                line_arr = line.split()
                stage = int(float(line_arr[-9]))
                if stage % 2 == 1:
                    shuffle_stages = int(line_arr[-1].split('/')[-1].split(')')[0])
                else:
                    result_stages = int(line_arr[-1].split('/')[-1].split(')')[0])


def add_column(mat, arr):
    if mat == []:
        mat = [[] for j in range(len(arr[1]))]
    for i in range(len(arr[1])):
        mat[i].append(arr[1][i])
    return mat

def add_dict_row(dict_row, all_times, dir_, root):
    if dict_row == {}:
        for i in range(len(dict_entries)):
            dict_row[dict_entries[i]] = []
        dict_row["sizes"] = []
        dict_row["worker_machine"] = []

    dict_row["Init"].append(all_times[0])
    dict_row["Job_0"].append(all_times[1])
    dict_row["Diff_0"].append(all_times[4])
    dict_row["Job_1"].append(all_times[2])
    dict_row["Diff_1"].append(all_times[5])
    dict_row["Job_2"].append(all_times[3])
    dict_row["Diff_2"].append(all_times[6])
    dict_row["Epoch_Sum"].append(all_times[7])
    dict_row["Differences"].append(all_times[8])
    dict_row["Last_diff"].append(all_times[9])

    f = open(root + '/' + dir_ + '/experiment', 'r')
    data = f.readlines()
    f.close()


    dir_arr = dir_.split('_')
    if 'synth' in dir_:
        f = open(root + '/' + dir_ + '/experiment', 'r')
        data = f.readlines()
        f.close()

        dict_row["worker_machine"].append(data[3].split()[1])


        synth_coord = dir_arr[-10] + '_' + dir_arr[-9]

        f = open('synth_dataset_sizes', 'r')
        data = f.readlines()
        f.close()

        data2 = filter(lambda x: synth_coord in x, data)
        idx = synth_list.index(root)


        dict_row["sizes"].append(ast.literal_eval(data2[idx].split()[1]))
    else:
        dict_row["sizes"].append(datasets_bytes[datasets.index(dir_arr[1])])


    return dict_row

def get_metrics_across_machine_counts(root):
    directories = get_directories(root)
    all_times = []
    for dir_ in directories:
        all_times = add_column(all_times, get_all_metrics(dir_, root, False, 0))
        dir_arr = dir_.split('_')
        if dir_arr[-8] == '16':
            print_all_times(dir_, all_times)
            all_times = []
        elif dir_arr[-8] == '8' and dir_arr[-9] in ['kdda', 'kddb']:
            print_all_times(dir_, all_times)
            all_times = []

def metric_means(matrix):
    transpose = map(list, zip(*matrix))
    collapsed = []
    for i, elem in enumerate(transpose):
        try:
            if isinstance(elem[0], list):
                mean_list = [np.mean(sub_elem) for sub_elem in map(list, zip(*elem))]
                collapsed.append([round(mean, 5) for mean in mean_list])
            else:
                collapsed.append(round(np.mean(elem), 5))
        except:
            sys.exit("We hit an error, debug it!")
    return collapsed


def get_metrics_across_lower_machine_counts(data_dict, empty_flag, root):
    directories = get_directories(root)
    all_times = []
    grouped_directories = [list(g) for k, g in groupby(directories, lambda s: s.split('_')[1])]

    if 'synth' in root:
        synth_grouped_dirs = []
        grouped_directories = [list(g) for k, g in groupby(directories, lambda s: s.split('_')[-10])]
        for dirs in grouped_directories:
            sub_groups = [list(g) for k, g in groupby(dirs, lambda s: s.split('_')[-9])]
            for sub_group in sub_groups:
                synth_grouped_dirs.append(sub_group)
        grouped_directories = synth_grouped_dirs
    for dirs in grouped_directories:

        for k, dir_ in enumerate(dirs):
            dir_arr = dir_.split('_')
            data_name = dir_arr[1]
            mach_num = dir_arr[-8]
            try:
                metric_arr = []
                all_time_arr = []
                for i in range(int(dir_arr[-2])):
                    all_time, metrics = get_all_metrics(dir_, root, False, i)
                    metric_arr.append(metrics)
                    all_time_arr.append(all_time)
                metric_arr = [round(np.mean(all_time_arr), 5), metric_means(metric_arr)]
            except Exception as inst:
                if data_name == 'synth':
                    data_name = 'synth ' + dir_arr[-10] + '_' + dir_arr[-9]
                print "There has been a mistake in creating the metrics array with dataset:", data_name
                print "With root dir:", root
                print "with dir_name", dir_
                # print type(inst)     # the exception instance
                # print inst           # __str__ allows args to be printed directly

                continue

            if data_name == 'synth':
                data_name = dir_arr[-10] + '_' + dir_arr[-9]

            all_times = add_column(all_times, metric_arr)
        if empty_flag == 0:
            data_dict[data_name] = add_dict_row({}, all_times, dir_, root)
        else:
            data_dict[data_name] = add_dict_row(data_dict[data_name], all_times, dir_, root)
        all_times = []


    return data_dict



def get_data_dict(data_directory_list):
    data_dict = {}
    data_dict = get_metrics_across_lower_machine_counts(data_dict, 0, data_directory_list[0])
    for elem in data_directory_list[1:]:
        data_dict = get_metrics_across_lower_machine_counts(data_dict, 1, elem)
    return data_dict


def pretty_print_data(data):
    pp = pprint.PrettyPrinter(indent=4)
    pp.pprint(data)

def main():
    data_dict = get_synth_data_dict(main_list)
    pretty_print_data_dict(data_dict)

if __name__ == "__main__":
    main()