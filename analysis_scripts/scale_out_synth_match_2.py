import sys
import ast
from matplotlib import pyplot as plt
import numpy as np
import math
import pandas as pd
import random
import os
from dateutil import parser
from matplotlib.font_manager import FontProperties

from scale_out_diff_overheads import *


synth_list = [
              'synth_cluster_scaling_exps_2_9_17_1000_samples_h1',
              'synth_ram_exp_.5gb_h1_2_13_17', 
              'synth_ram_exp_1gb_h1_2_13_17', 
              'synth_ram_exp_2gb_h1_2_13_17',
             ]

real_list = [
            'exps_1_17_17_all_data',
            'm1_min_2_7_17_all_data',
            'hi1_min_2_7_17_all_data',
           ]

machine_names = [
                 'cg1.4xlarge',
                 'm1.large',
                 'hi1.4xlarge',
                ]

machine_ram = [4, 11.5, 35]

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
    try:
        for directory in os.walk(root):
            directories = directory[1]
            break
    except:
        directories = ''
    return directories

def check_args(dataset): #, dist_type):
    if dataset not in ['higgs', 'susy', 'url', 'kdda', 'kddb']:
        sys.exit("Not a valid dataset.")
    # if dist_type not in ['linear', 'exp']:
    #     sys.exit("Not a valid distance function type.")

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


def make_tex_table(matrix_t):
    table_str = r'\begin{table} \begin{tabular}{' + '|l|'*len(matrix_t[1])+ '}  \hline '
    for i in range(len(matrix_t)):
        str_to_add = ' ' + str(matrix_t[i][0]) + ' '
        table_str += str_to_add
        for j in range(len(matrix_t[0]) - 1):
            str_to_add = ' & ' + str(matrix_t[i][0]) + ' '
            table_str += str_to_add
        table_str += ' \\ \hline ' 
    table_str += ' \end{tabular} \end{table}'
    return table_str

def transform_func(point, entry, dataset_name):
    if entry == 'Epoch_Sum' and dataset_name in ['higgs', 'url']:
        return math.log10(point)
    else:
        return point

def log_zero(entry, dataset_name):
    if entry == 'Epoch_Sum' and dataset_name in ['higgs', 'url']:
        return 1
    else:
        return 0

def axis_name_change(name, entry, dataset_name):
    if entry == 'Epoch_Sum' and dataset_name in ['higgs', 'url']:
        return "Log10 of Time(s)"
    else:
        return name

def plot_the_lot(dataset_name, synth_data_dict, real_data_dict, coordinates, machine_num, data_set_num):
    color_array = ['blue', 'red', 'black', 'green', 'purple']
    width = 0.35
    ind = map(lambda x: x.split('_')[-1], synth_list)

    for entry in filtered_dict_entries:
        fig, ((ax1, ax2), (ax3, ax4), (ax7, ax8), (ax9, ax10), (ax5, ax6)) = plt.subplots(5, 2)
        fig.suptitle(entry.upper() + " for " + dataset_name.upper() + " dataset", fontsize=14)

        ax_list = [ax1, ax3, ax7, ax9]
        col_lens = []
        rects_list = []
        for i in range(len(coordinates)):
            coord_str = str(coordinates[i][0]) + '_' + str(coordinates[i][1])
            ax_list[i].set_title(coord_str)

            matrix = synth_data_dict[coord_str][entry]
            offset = synth_data_dict[coord_str]['label_offset']
            diff_mach_nums = len(matrix[0])
            col_lens.append(max([len(matrix[j]) for j in range(len(matrix))]))

            rect_list = []
            for j in range(diff_mach_nums):
                matrix_t = map(list, zip(*matrix))
                try:
                    rect_list.append(ax_list[i].bar([2*(k+.25) + j*width for k in range(len(matrix))], matrix_t[j], width, color=color_array[j+offset]))
                except:
                    continue
            rects_list.append(rect_list)

            ax_list[i].set_ylabel('Time (s)')
            ax_list[i].set_xticks([2*(k+.25) + diff_mach_nums*width/2 for k in range(len(matrix))])
            ax_list[i].set_xticklabels(ind[:len(matrix)])

            box = ax_list[i].get_position()
            ax_list[i].set_position([box.x0, box.y0 + box.height * 0.1,
                             box.width, box.height * 0.9])

        max_machs = max(col_lens)
        rect_list = max(rects_list, key=len)
        ax_list[0].legend(map(lambda x: x[0], rect_list), [str(2**j) for j in range(max_machs)], loc='upper center', 
                          bbox_to_anchor=(1.5, -.1), fancybox=True, shadow=True, ncol=machine_num, title="Machine Counts")

        ax_list = [ax2, ax4, ax8, ax10]
        for i in range(len(coordinates)):
            coord_str = str(coordinates[i][0]) + '_' + str(coordinates[i][1])

            sizes_in_bytes = synth_data_dict[coord_str]['sizes']
            #FIX THIS LATER, it shouldn't be machine[0], but name above each bar

            machine = synth_data_dict[coord_str]['worker_machine']
            ax_list[i].set_title(coord_str + " RAM Ratios")

            matrix = synth_data_dict[coord_str][entry]
            diff_mach_nums = len(matrix[0])
            dataset_count = len(matrix)
            mach_indices = [machine_names.index(mach) for mach in machine]
            ram_arr = [machine_ram[mach_idx] for mach_idx in mach_indices]
            ratios = []

            #CHANGE TOMORROW!
            #for ram in machine_ram[3-len(matrix):]:
            for k, dbytes in enumerate(sizes_in_bytes):
                ratio = []
                for j in range(diff_mach_nums):
                    #ratio.append(math.log10(dbytes/(ram_arr[k]*10e9*(2**j))))
                    ratio.append(dbytes/(ram_arr[k]*1e9*(2**j)))
                ratios.append(ratio)

            rects_list = []
            for j in range(diff_mach_nums):
                ratios_t = map(list, zip(*ratios))
                try:
                    rects_list.append(ax_list[i].bar([2*(k+.25) + j*width for k in range(len(sizes_in_bytes))], ratios_t[j], width, color=color_array[j]))
                except:
                    continue
            ax_list[i].set_ylabel('Ratios')
            ax_list[i].set_xticks([2*(k+.25) + diff_mach_nums*width/2 for k in range(len(matrix))])
            labels = []
            for j in range(len(machine)):
                labels.append(ind[:len(ratios)][j] + ' ' + machine[j])
            ax_list[i].set_xticklabels(labels)

            box = ax_list[i].get_position()
            ax_list[i].set_position([box.x0, box.y0 + box.height * 0.1,
                             box.width, box.height * 0.9])

        ax5.set_title(dataset_name)
        matrix = real_data_dict[dataset_name][entry]

        offset = real_data_dict[dataset_name]['label_offset']
        diff_mach_nums = len(matrix[0])
        max_val = max([len(matrix[j]) for j in range(len(matrix))])
        fill_in_matrix = []
        for elem in matrix:
            if len(elem) < max_val:
                for k in range(max_val - len(elem)):
                    elem.append(log_zero(entry, dataset_name))
            fill_in_matrix.append(elem)

        real_rects_list = []
        matrix_t = map(list, zip(*fill_in_matrix))

        for j in range(diff_mach_nums):
            real_rects_list.append(ax5.bar([k+.75 + j*width/4 for k in range(len(fill_in_matrix))], [transform_func(scal, entry, dataset_name) for scal in matrix_t[j]], width/4, color=color_array[j+offset]))

        ax5.set_ylabel(axis_name_change('Time (s)', entry, dataset_name))
        ax5.set_xticks([k+.75 + diff_mach_nums*width/8 for k in range(len(matrix))])


        ax5.set_xticklabels(machine_names[3-len(matrix):])

        box = ax5.get_position()
        ax5.set_position([box.x0, box.y0 + box.height * 0.05,
                         box.width/2, box.height * 1.2])

        #plt.subplot2grid((3,3), (2,0))

        # table_str = make_tex_table(matrix_t)
        dataset_idx = datasets.index(dataset_name)
        ram_ratios = []

        #CHANGE TOMORROW!
        #for ram in machine_ram[3-len(matrix):]:
        for ram in machine_ram:
            ram_ratio = []
            for j in range(diff_mach_nums):
                ram_ratio.append(datasets_bytes[dataset_idx]/(ram*(2**j)))
            ram_ratios.append(ram_ratio)

        ratio_rect_list = []
        for j in range(diff_mach_nums):
            ram_ratios_t = map(list, zip(*ram_ratios))
            ratio_rect_list.append(ax6.bar([k+.75 + j*width/4 for k in range(len(ram_ratios))], ram_ratios_t[j], width/4, color=color_array[j+offset]))

        ax6.set_ylabel('Ratio', fontsize=10)
        ax6.set_title('Dataset Size in GB(s) / ( RAM on Machine * # of Machines )', fontsize=12)
        ax6.set_xticks([k+.75 + diff_mach_nums*width/8 for k in range(len(ram_ratios))])
        ax6.set_xticklabels(machine_names)

        box = ax6.get_position()
        ax6.set_position([box.x0, box.y0 + box.height * 0.05,
                         box.width/2, box.height * 1.2])

        #CHANGE TOMORROW!
        #ax6.set_xticklabels(machine_names[3-len(ram_ratios):])
        #plt.rc('text', usetex=True)

        plt.subplots_adjust(wspace=1, hspace=.75)
        plt.show()


def main():

    averages = 'no'

    clp = sys.argv
    try:
        dataset = clp[1]
    except:
        1

    check_args(dataset)

    synth_data_dict = get_data_dict(synth_list)
    real_data_dict = get_data_dict(real_list)

    #coordinates, synth_coordinates = get_synth_coordinates(dataset)
    coordinates, synth_coordinates = get_synth_coordinates_2(dataset)
    plot_the_lot(dataset, synth_data_dict, real_data_dict, synth_coordinates, 5, len(synth_list))

    # synth_dirs = get_directories('synth_exps_100k_1_12_17')
    # dir_tuples = get_synth_dirs(machines, synth_coordinates, synth_dirs)
    #closest_synths_epochs = get_synth_epochs(dir_tuples)


    # print repr(names)
    # color_array = ['blue', 'red', 'purple', 'black']
    # for i in range(len(epos)):
    #     plt.plot(epos[i], color_array[i])

    # plt.show()

    # closest_synths_diffs = get_synth_diffs(dir_tuples)

    # if averages != 'no':
    #     get_averages(dist_type, coordinates, exp, closest_synths_epochs, closest_synths_diffs)
    # else:
    #     f = open('exps_1_16_17/' + exp + '/epochs', 'r')
    #     epochs_500 = ast.literal_eval(f.readlines()[0])
    #     f.close()
    #     synth_match(epochs_500, closest_synths_epochs, coordinates, dist_type, True)
    #     diff_500 = get_diff(exp, 'exps_1_16_17', 0)
    #     synth_match(diff_500, closest_synths_diffs, coordinates, dist_type, True)

if __name__ == "__main__":
    main()


