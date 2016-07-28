import os
import sys
import numpy as np
import matplotlib.pyplot as plt
from matplotlib import colors
import six
from random import randint

#Dependency Files
import color_file


def walk_directories_to_process(root):
    for dirpath, dirname, filenames in os.walk(root):
        if dirpath == root:
            continue
        else:
            print "In directory:", dirpath
            for file in filenames:
                print "Processing", file
                file_path = os.path.join(dirpath, file)
                parse_run_log_file(file_path)


def parse_run_log_file(path):
    f1 = open(path, 'r')
    f2 = None
    lines = f1.readlines()
    flag = 0
    curr_epoch = 0
    for line in lines:
        line_array = line.split()
        if len(line_array) == 0:
            continue
        if line_array[0] == 'Epoch':
            flag = 1
            f2 = open(path + '_processed', 'w')
        if flag == 0:
            continue
        f2.write(line)
        if line_array[0] == 'Epoch':
            continue
        if curr_epoch == int(line_array[0]):
            break
        else:
            curr_epoch = int(line_array[0])




def mean_filters(time_matrix):
    unique_mach_values = list(set(map(lambda x: x[1], time_matrix)))
    unique_mach_values.sort

    filtered_average_time_matrix = []
    for value in unique_mach_values:
        filtered_time_matrix = filter(lambda x: x[1] == value, time_matrix)
        runs = len(filtered_time_matrix)
        epochs = len(filtered_time_matrix[0][0])
        average_time_array = []
        for i in range(epochs):
            average_epoch_timing = sum(map(lambda x: x[0][i], filtered_time_matrix))/runs
            average_time_array.append(average_epoch_timing)
        filtered_average_time_matrix.append((average_time_array, value))
    return filtered_average_time_matrix



def walk_directories_to_graph(root):
    for dirpath, dirname, filenames in os.walk(root):
        if dirpath == root:
            continue
        else:
            print "In directory:", dirpath
            time_matrix = []

            for file in filenames:
                if 'processed' not in file:
                    continue
                file_path = os.path.join(dirpath, file)
                f = open(file_path, 'r')
                time_array = map(lambda x: float(x.split()[7]), f.readlines()[1:])
                epoch_duration_array = [time_array[0]]
                for i in range(1,len(time_array)):
                    epoch_duration_array.append(time_array[i] - time_array[i-1])
                time_matrix.append((epoch_duration_array, file.split('_')[-4]))

            filtered_average_time_matrix = mean_filters(time_matrix)
            if len(filtered_average_time_matrix) == 0:
                continue
            print "Graphing", dirpath.split('/')[-1]
            epochs = len(time_matrix[0][0])
            plot_array = []
            idx = 0
            for arr, num_machs in filtered_average_time_matrix:
                t = np.arange(0, epochs, 1)
                #arr.insert(0, 0.0)
                plot_array.append((t[:-1], arr[:-1], colors_[idx % len(colors_)], num_machs))
                idx += 1
            for pv in plot_array:
                plt.plot(pv[0], pv[1], pv[2], label=pv[3])
            plt.legend()
            plt.xlabel('Epochs')
            plt.ylabel('Epoch Durations (Seconds)')
            plt.title(dirpath.split('/')[-1])
            plt.show()




path_to_walk = '/Users/Kevin/MLaaS/MLaaS/ubuntu_scripts_for_experiment/experiment_data'

#PROCESS FILES
#walk_directories_to_process(path_to_walk)

#GRAPH FILES
walk_directories_to_graph(path_to_walk)