import sys
import os
import math
from scipy.interpolate import interp1d
import ast
import numpy as np
import matplotlib.pyplot as plt

from scale_out_diff_overheads import *


communication_dir = 'synth_cluster_scaling_exps_2_9_17_h1_30_trials'
computation_dirs = [
                    'synth_ram_exp_.5gb_h1_2_13_17_30_trials',
                    'synth_ram_exp_1gb_h1_2_13_17_30_trials',
                    'synth_ram_exp_2gb_h1_2_13_17_30_trials',
                    'synth_ram_exp_4gb_h1_2_13_17_30_trials',
                    'synth_ram_exp_8gb_h1_2_13_17_30_trials',
                   ]

def get_log(arr):
    return math.log(arr, 10)

def get_subdirs(root):
    directories = ''
    try:
        for directory in os.walk(root):
            directories = directory[1]
            break
    except:
        print "Could not get subdirs for root:", root
    return directories

def filter_dirs_and_extract_times(ret_str, root, dir_arr, machines, features):
    dir_ = filter(lambda x: str(machines) + '_machine' in x, dir_arr)
    dir_ = filter(lambda x: 'data_' + str(features) in x, dir_)[0]
    exp_path = root
    epoch_dir = dir_
    f = open(exp_path + '/' + epoch_dir +'/all_iters')
    num = len(f.readlines())
    f.close()
    all_time_arr = []
    metrics_arr = []
    arrs_of_arrs = []
    for idx in range(num):
        all_time, metrics, arrs = get_all_metrics_modified(epoch_dir, exp_path, False, idx)
        all_time_arr.append(all_time)
        metrics_arr.append(metrics)
        arrs_of_arrs.append(arrs)

    shuff_tasks, res_tasks = get_task_counts(epoch_dir, exp_path)


    #metrics_arr_t = map(list, zip(*metrics))

    idx = all_time_arr.index(sorted(all_time_arr)[len(all_time_arr)/2])

    median_metrics = [shuff_tasks, res_tasks] + metrics_arr[idx] + arrs_of_arrs[idx]


    # f = open(path, 'r') 
    # data = f.readlines()
    # median_time = np.median([ast.literal_eval(elem) for elem in data])

    return all_time_arr[idx], median_metrics


def diff_two_lists(arr1, arr2):
    return [x - y for x,y in zip(arr1, arr2)]

def get_real_times(dataset, machines, comm_mets):

    root = 'real_data/' + dataset
    subdirs = get_subdirs(root)
    dir_ = filter(lambda x: str(machines) + '_machine' in x, subdirs)[0]
    all_time, all_mets, arrs = get_all_metrics_modified(dir_, root, False, 0)
    shuff_tasks, res_tasks = get_task_counts(dir_, root)
    metrics = [shuff_tasks, res_tasks] + all_mets + arrs

    comp_mets = diff_two_lists(metrics[:-5], comm_mets[:-5]) + [diff_two_lists(x, y) for x,y in zip(metrics[-5:], comm_mets[-5:])]

    return comp_mets, metrics

def get_times(features, machines, mem):
    comm_subdirs = get_subdirs(communication_dir)
    computation_dir = computation_dirs[0]
    for dir_ in computation_dirs[1:]:
        mem_str = str(int(mem)) + 'gb'
        if mem_str in dir_:
            computation_dir = dir_
    comp_subdirs = get_subdirs(computation_dir)
    comm, comm_metrics = filter_dirs_and_extract_times('comm', communication_dir, comm_subdirs, machines, features)
    all_, all_mets= filter_dirs_and_extract_times('', computation_dir, comp_subdirs, machines, features)
    comp = all_ - comm

    comp_mets = diff_two_lists(all_mets[:-5], comm_metrics[:-5]) + [diff_two_lists(x, y) for x,y in zip(all_mets[-5:], comm_metrics[-5:])]
    return comm, comp, comm_metrics, comp_mets, all_, all_mets


def transform_samples(features, machines, mem, old_val, new_val, prev_communication, prev_computation):
    return prev_communication, prev_computation*(float(new_sample_size)/old_sample_size - 1)



def log_linear_interpolation(log_x, y_data, value, log_base):
    log_y = get_log(data_y)
    interpolation = interp1d(log_x, log_y)
    return log_base**(interpolation(math.log(value, log_base))[0])

def transform_features(features, machines, mem, prev_communication, prev_computation):
    log_feats = math.log(features, 10)
    lower_feats = int(math.floor(log_feats))
    upper_feats = int(math.ceil(log_feats))
    comm1, comp1 = get_times(lower_feats, machines, mem)
    comm2, comp2 = get_times(upper_feats, machines, mem)
    itpl_comm = log_linear_interpolation([lower_feats, upper_feats], [comm1, comm2], features, 10)
    itpl_comp = log_linear_interpolation([lower_feats, upper_feats], [comp1, comp2], features, 10)


    arr = math.fabs(lower_feats, log_feats), math.fabs(upper_feats, log_feats)
    min(arr)

    return  

def transform_machines(features, machines, mem, old_val, new_val, prev_communication, prev_computation):
    1

def transform_epochs(features, machines, mem, old_val, new_val, prev_communication, prev_computation):
    1

def transform_bytes(features, machines, mem, prev_communication, prev_computation):
    log_mem = math.log(mem, 2)
    lower_mem = int(math.floor(log_mem))
    upper_mem= int(math.ceil(log_mem))
    comm1, comp1 = get_times(features, machines, lower_mem)
    comm2, comp2 = get_times(features, machines, upper_mem)
    itpl_comm = log_linear_interpolation([lower_mem, upper_mem], [comm1, comm2], mem, 2)
    itpl_comp = log_linear_interpolation([lower_mem, upper_mem], [comp1, comp2], mem, 2)


    arr = math.fabs(lower_mem, log_mem), math.fabs(upper_mem, log_mem)
    min(arr)

    return


features = [i for i in range(1,9)]
gbs = [.5, 1, 2, 4, 8]
machs = [2,4,8]


def check_diffs(arr, print_str):
    val = arr[1] - arr[0]
    print print_str
    print "Diff between 4 and 2 machines:", val
    print "Diff between 8 and 4 machines:", arr[2] - arr[1]
    print "Diff between 8 and 2 machines:", arr[2] - arr[0]
    print "Diff between 4 and 2 machines multiplied:", val, val*2, val*3
    print


def round_time(arr):
    time = arr
    for i in range(len(time)):
        if not isinstance(time[i], list):
            time[i] = round(time[i], 5)
        else:
            for j in range(len(time[i])):
                time[i][j] = round(time[i][j], 5)

        return time

def get_all_times(gb, comm_bool, comp_bool):
    f = open('experiment_timings/sizes', 'r')
    sizes = [int(line) for line in f.readlines()]
    f.close()
    for feature in features:
        times = []
        shuff_tasks = []
        res_tasks = []
        for mach in machs:
            comm_time, comp_time, comm_mets, comp_mets, all_, all_mets, shuffle_tasks, result_tasks = get_times(feature, mach, gb)
            time = None
            if comm_bool == True:
                time = comm_mets
            elif comp_bool == True:
                time = comp_mets
            else:
                time = all_mets
            time = round_time(time)
            times.append(time)
            shuff_tasks.append(shuffle_tasks)
            res_tasks.append(result_tasks)

        print
        print "FEATURES: 10 ^", str(feature)
        print "Machine Count:", repr([2, 4, 8])
        print "Core Count:", repr([16, 32, 64])

        size = 1
        if comm_bool == False:
            size = sizes[gbs.index(gb)*8 + feature - 1]
            print "Shuffle Task Count:", repr(shuff_tasks)
            print "Result Task Count:", repr(res_tasks)

        print "Sample Count:", str(size)

        all_times = map(list, zip(*times))
        print
        print "Init:", repr(all_times[0])
        print "Job 0:", repr(all_times[1])
        print "Diff 0:", repr(all_times[4])
        print "Job 1:", repr(all_times[2])
        print "Diff 1:", repr(all_times[5])
        print "Job 2:", repr(all_times[3])
        print "Diff 2:", repr(all_times[6])
        print "Shuffle Stage Sum:", repr(all_times[7])
        print "Result Stage Sum:", repr(all_times[8])
        print "Internal Epoch Difference Sum:", repr(all_times[9])
        print "Epoch Sum:", repr(all_times[10])
        print "Differences:", repr(all_times[11])
        print "Last_diff:", repr(all_times[12])
        print "ShuffleMapStage Time Array:", repr(all_times[13])
        print "ResultStage Time Array:", repr(all_times[14])
        print "Internal Epoch Diffs Array:", repr(all_times[15])
        print "Epoch Array:", repr(all_times[16])
        print "Diffs Array:", repr(all_times[17])
        print
        print
        print

real_dataset_names = ['higgs', 'susy', 'url', 'kdda', 'kddb']
real_samples = [11000000,5000000,2396130,8407752,19264097]
real_features = [28,18,3231961,20216830,29890095]
real_bytes = [7.4, 2.3, 2.1, 2.5, 4.8]



def compare_real_times(dataset, show, print_show, mach_plot):
    f = open('experiment_timings/sizes', 'r')
    sizes = [int(line) for line in f.readlines()]
    f.close()

    idx = real_dataset_names.index(dataset)
    feature = int(np.round(math.log(real_features[idx], 10)))
    gb = 2**int(np.round(math.log(real_bytes[idx], 2)))



    comm_times = []
    synth_all_times = []
    synth_comp_times = []
    real_comp_times = []
    real_all_times = []

    for mach in machs:
        comm_time, comp_time, comm_mets, comp_mets, all_, all_mets = get_times(feature, mach, gb)


        comm_times.append(round_time(comm_mets))
        synth_all_times.append(round_time(all_mets))
        synth_comp_times.append(round_time(comp_mets))


        real_comp_mets, real_all_mets = get_real_times(dataset, mach, comm_mets)

        real_comp_times.append(round_time(real_comp_mets))
        real_all_times.append(round_time(real_all_mets))

    if print_show == True:
        datasets = ['Communication Time', 'Nearest Synthetic Neighbor All Times', 'Nearest Synthetic Neighbor Computation Times', dataset + ' full time', dataset + ' computation time']
        for k, data in enumerate([comm_times, synth_all_times, synth_comp_times, real_all_times, real_comp_times]):

            all_times = map(list, zip(*data))

            print
            print "Dataset:", datasets[k]
            print "Features:", str(real_features[idx])
            print "Machine Count:", repr([2, 4, 8])
            print "Core Count:", repr([16, 32, 64])

            size = 1
            if k < 2:
                if k == 1:
                    size = sizes[gbs.index(gb)*8 + feature - 1]
            else:
                size = real_samples[idx]

            print "Shuffle Task Count:", repr(all_times[0])
            print "Result Task Count:", repr(all_times[1])

            print "Sample Count:", str(size)
            print "Byte Count (GB):", str(real_bytes[idx])

            
            print
            print "Init:", repr(all_times[2])
            print "Job 0:", repr(all_times[3])
            print "Diff 0:", repr(all_times[6])
            print "Job 1:", repr(all_times[4])
            print "Diff 1:", repr(all_times[7])
            print "Job 2:", repr(all_times[5])
            print "Diff 2:", repr(all_times[8])
            print "Shuffle Stage Sum:", repr(all_times[9])
            print "Result Stage Sum:", repr(all_times[10])
            print "Internal Epoch Difference Sum:", repr(all_times[11])
            print "Epoch Sum:", repr(all_times[12])
            print "Differences:", repr(all_times[13])
            print "Last_diff:", repr(all_times[14])
            print "ShuffleMapStage Time Array:", repr(all_times[15])
            print "ResultStage Time Array:", repr(all_times[16])
            print "Internal Epoch Diffs Array:", repr(all_times[17])
            print "Epoch Array:", repr(all_times[18])
            print "Diffs Array:", repr(all_times[19])
            print
            print
            print

    if show == True:
        comm = map(list, zip(*comm_times))
        synth_comp = map(list, zip(*synth_comp_times))
        real_comp = map(list, zip(*real_comp_times))

        synth_all = map(list, zip(*synth_all_times))
        real_all = map(list, zip(*real_all_times))

        fig, (ax1, ax2, ax3) = plt.subplots(3, 1, sharex=True)
        ax1.set_title('Epoch Times')
        mach_plot_idx = int(math.log(mach_plot, 2)) - 1
        #ax1.plot(synth_all[18][mach_plot_idx], color='black')
        ax1.plot(real_all[18][mach_plot_idx], color='red')

        real_epoch_mean = np.mean(real_all[18][mach_plot_idx][200:])
        ax1.plot([real_epoch_mean for j in range(500)], color='black')

        # synth_epoch_mean = np.mean(synth_all[18][mach_plot_idx][200:])
        # ax1.plot([synth_epoch_mean for j in range(500)], color='gray')

        ax2.set_title('Overheads + Communication Time')
        ax2.plot(comm[18][mach_plot_idx])

        comm_mean = np.mean(comm[18][mach_plot_idx][200:])
        ax2.plot([comm_mean for j in range(500)], color='cyan')


        ax3.set_title('Computation Times')
        #ax3.plot(synth_comp[18][mach_plot_idx], color = 'black')
        ax3.plot(real_comp[18][mach_plot_idx], color = 'red')

        real_comp_mean = np.mean(real_comp[18][mach_plot_idx][200:])
        ax3.plot([real_comp_mean for j in range(500)], color='black')

        estimate = np.median(real_comp[18][mach_plot_idx][10:50])
        actual = sum(real_comp[18][mach_plot_idx])
        estimated = sum(real_comp[18][mach_plot_idx][:50]) + estimate*450
        perc_error = (math.fabs(actual - estimated)/math.fabs(actual)) * 100

        print "Actual:", actual
        print "Estimated:", estimated
        print "Percent error:", perc_error

        props = dict(boxstyle='round', facecolor='wheat', alpha=0.5)
        textstr = 'Absolute Percent Error:  %.5f%%'%(perc_error)
        text_box = ax2.text(.5, 0.95, textstr, transform=ax2.transAxes, fontsize=14,
            verticalalignment='top', bbox=props)

        # synth_comp_mean = np.mean(synth_comp[18][mach_plot_idx][200:])
        # ax3.plot([synth_comp_mean for j in range(500)], color='gray')


        fig.suptitle("Dataset: " + dataset + ",  Machine Count: " + str(mach_plot), fontsize=18)
        plt.show()

clp = sys.argv

val1 = ast.literal_eval(clp[2])
val2 = ast.literal_eval(clp[3])

if clp[4] == 'synth':
    get_all_times(float(clp[1]), val1, val2)

if clp[4] == 'real':
    dataset = clp[1]
    os.system('mkdir experiment_timings/' + dataset)
    compare_real_times(dataset, val1, val2, int(clp[5]))



