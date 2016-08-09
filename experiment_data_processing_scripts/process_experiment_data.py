import os
import sys
import numpy as np
import matplotlib.pyplot as plt
from matplotlib import colors
import six
from random import randint
import glob

from subprocess import Popen, PIPE
from time import sleep


#Dependency Files
import color_file


def walk_directories_to_process(root, out_file):
    for dirpath, dirname, filenames in os.walk(root):
        if dirpath == root:
            continue
        else:
            print "In directory:", dirpath
            for file in filenames:
                file_path = os.path.join(dirpath, file)
                if ('run_1_' in file_path) and ('_epoch_1_' in file_path) and (int(file_path.split('_')[-1]) == 1):
                    print "Processing", file
                    parse_run_out_file(file_path, out_file)

def get_num_files(path):
    return len(glob.glob(path + '*'))

def read_epoch_time(path):
    f = open(path, 'r')
    data = f.readlines()
    delimit = '********************************************************\n'
    idx = -1
    if delimit in data:
        idx = data.index(delimit)
    else:
        print 'WE HAD AN ERROR DURING THE EXPERIMENT, RETRAIN'
    if idx > -1:
        epoch_time = float(data[idx - 2].split()[-1])
        return epoch_time
    else:
        sys.exit('AN ERROR OCCURRED WHHILE TRYING TO READ THE EPOCH TIME')
        return -1

def create_file_path(path_prefix, num_machs, run, epoch, sub_epoch):
    path = [path_prefix + '_' + str(num_machs) +
           '_run_' + str(run + 1) +
           '_epoch_' + str(epoch + 1) +
           '_sub-epoch_' + str(sub_epoch + 1)]
    return path[0]

def create_mach_file(write_file, num_epochs):
    f = open(write_file, 'w')
    f.write('Epoch\n')
    for epoch in range(num_epochs):
        f.write(str(epoch + 1) + '\n')
    f.close()

def write_mach_column_to_file(write_file, mach_epoch_times, num_machs):
    f = open(write_file, 'r')
    data = f.readlines()
    f.close()
    f = open(write_file, 'w')
    f.write(data[0].strip() + '\t' + str(num_machs) + '\n')
    for (i,line) in enumerate(data[1:]):
        f.write(line.strip() + '\t' + str(mach_epoch_times[i]) + '\n')
    f.close()

def parse_run_out_file(path, out_file):
    mach_name = path.split('/')[-1].split('_')[0]
    split_path = path.split('_')
    write_file = out_file + '/' + mach_name

    num_sub_epochs = get_num_files('_'.join(split_path[:-1]))
    num_epochs = get_num_files('_'.join(split_path[:-3])) / num_sub_epochs
    num_runs = get_num_files('_'.join(split_path[:-5])) / (num_sub_epochs * num_epochs)
    num_machine_configs = get_num_files('_'.join(split_path[:-7])) / (num_sub_epochs * num_epochs * num_runs)
    print num_sub_epochs, num_epochs, num_runs, num_machine_configs

    path_prefix = '_'.join(split_path[:-7])
    create_mach_file(write_file, num_epochs)

    for mach_conf in range(num_machine_configs):
        num_machs = 2 ** mach_conf
        run_array = []
        for run in range(num_runs):
            epoch_times = []
            for epoch in range(num_epochs):
                sub_epoch_arr = []
                for sub_epoch in range(num_sub_epochs):
                    try:
                        file_path = create_file_path(path_prefix, num_machs, run, epoch, sub_epoch)
                        sub_epoch_time = read_epoch_time(file_path)
                        sub_epoch_arr.append(sub_epoch_time)
                    except:
                        print file_path, ' DOES NOT EXIST'
                epoch_times.append(sum(sub_epoch_arr))
            run_array.append(epoch_times)

        mach_epoch_times = [float(sum(x))/num_runs for x in zip(*run_array)]
        write_mach_column_to_file(write_file, mach_epoch_times, num_machs)





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


def create_html_files_from_processed_data(path):
    path_array =  path.split('/')
    out_dir = '/'.join(path_array[:-2]) + '/experiment_html_charts'
    html_dir = out_dir + '/' + path_array[-1]
    os.system('mkdir ' + html_dir)

    for dirpath, dirname, filenames in os.walk(path):
        print filenames
        for file in filenames:
            file_path = os.path.join(dirpath, file)
            f2 = open(file_path, 'r')
            data = f2.readlines()

            f = open('tmp', 'w')
            for (i,line) in enumerate(data):
                line = line.strip()
                if line != '':
                    arr = line.split('\t')
                    if i != 0:
                        arr = [arr[0]] + map(lambda x: float(x), arr[1:])
                    f.write(repr(arr) + ',\n')
            f.write(']);\n\n')
            f.write('var options = {\n')
            f.write('title: "' + file + '",\n')
            f.write('curveType: "function",\n')
            f.write('legend: { position: "bottom" }\n};\n')
            os.system('cat html1 tmp html2 > ' + html_dir + '/' + file + '.html')
            os.system('rm tmp')

def main():
    clp = sys.argv
    data_set = clp[1]
    run = clp[2]

    path_to_walk = '/Users/Kevin/MLSchedule/experiment_data/' + data_set + '/' + run
    out_file = '/Users/Kevin/MLSchedule/experiment_data_processing_scripts/processed_experiments/epoch-times-' + run
    os.system('mkdir ' + out_file)
    #PROCESS FILES
    walk_directories_to_process(path_to_walk, out_file)
    create_html_files_from_processed_data(out_file)

    #GRAPH FILES
    #walk_directories_to_graph(path_to_walk)

if __name__ == "__main__":
    main()

