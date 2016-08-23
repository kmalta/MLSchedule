import os
import sys
import numpy as np
import matplotlib.pyplot as plt
from matplotlib import colors
import six
from random import randint
import glob
import math

from subprocess import Popen, PIPE
from time import sleep


#Dependency Files
import color_file

def walk_directories_to_process(root, out_file, staleness):
    for dirpath, dirname, filenames in os.walk(root):
        if dirpath == root:
            continue
        else:
            print "In directory:", dirpath
            for file in filenames:
                file_path = os.path.join(dirpath, file)
                if 'run' in file_path and 'staleness_0' in file_path and int(file_path.split('_')[-1]) == 1:
                    print "Processing", file
                    parse_run_out_file(root, file_path, out_file, staleness)

def get_num_files(path):
    return len(glob.glob(path + '*'))


def create_file_path(path_prefix, num_machs, stale, run):
    path = [path_prefix + '_' + str(num_machs) +
           '_staleness_' + str(stale) +
           '_run_' + str(run + 1)]
    return path[0]

def create_mach_file(write_file, mach_name, num_epochs, stale, num_machs, type_):
    path = write_file + '_staleness_' + str(stale) + '_machines_' + str(num_machs)
    f = open(path, 'w')
    f.write(str(mach_name) + '\t' + str(num_epochs) + '\t' + str(stale) + '\t' + str(num_machs) + '\n')
    if type_ == 'epoch':
        for epoch in range(num_epochs):
            f.write(str(epoch + 1) + '\n')
    f.close()
    return path


def write_data_stats(path, features, labels, train_data_size):
    f = open(path, 'w')
    f.write(str(features) + '\t' + str(labels) + '\t' + str(train_data_size) + '\n')
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

def write_epoch_times_to_file(write_file, epoch_times):
    f = open(write_file, 'r')
    data = f.readlines()
    f.close()
    f = open(write_file, 'w')
    f.write(data[0])
    for (i, time) in enumerate(epoch_times):
        f.write(data[i + 1].strip() + '\t' + str(time) + '\n')
    f.close()

def write_overhead_times_to_file(write_file, ml_overhead):
    f = open(write_file, 'r')
    data = f.readlines()
    f.close()
    f = open(write_file, 'w')
    f.write(data[0])
    stub = ''
    try:
        stub = data[1].strip()
    except:
        1
    f.write(stub + '\t' + str(ml_overhead))
    f.close()

def get_num_epochs(path):
    f = open(path, 'r')
    data = f.readlines()
    idx = -1
    for (i, line) in enumerate(data):
        if '****' in line:
            idx = i - 2
    return int(data[idx].split()[0])



def read_epoch_times(file_path, num_epochs):
    f = open(file_path, 'r')
    epochs = []
    idx = 17
    data = f.readlines()
    for i in range(num_epochs):
        epochs.append(float(data[idx + i].split()[-1]))

    return float(data[-1].split()[-1]), epochs



def parse_run_out_file(root, path, out_file, staleness):
    mach_name = path.split('/')[-1].split('_')[0]
    split_path = path.split('_')

    dir_path = out_file + '/' + mach_name
    os.system('mkdir ' + dir_path)

    #CONSTANTS, change these from hardcode
    runs = 30
    num_epochs = get_num_epochs(path)
    #especially this one
    machs = {}
    for dirpath, dirname, filenames in os.walk(root):
        machine_name = dirpath.split('/')[-1]
        machs[machine_name] = []
        for file in filenames:
            machs[machine_name].append(int(file.split('_')[-5]))
        machs[machine_name] = list(set(machs[machine_name]))
        machs[machine_name].sort()

    path_prefix = '_'.join(split_path[:-5])

    for stale in staleness:
        for num_machs in machs[mach_name]:


            epoch_file_path = create_mach_file(dir_path+ '/epoch_times_' + mach_name, mach_name, num_epochs, stale, num_machs, 'epoch')
            overhead_file_path = create_mach_file(dir_path + '/ml_overhead_' + mach_name, mach_name, num_epochs, stale, num_machs, 'overhead')

            for run in range(runs):
                epoch_times = []
                
                file_path = create_file_path(path_prefix, num_machs, stale, run)
                ml_time, epoch_times = read_epoch_times(file_path, num_epochs)

                ml_overhead = ml_time - epoch_times[-1]

                write_epoch_times_to_file(epoch_file_path, epoch_times)
                write_overhead_times_to_file(overhead_file_path, ml_overhead)




def create_epoch_html_files_from_processed_data(path, num_chunks, used_chunks):

    path_array =  path.split('/')
    out_dir = '/'.join(path_array[:-2]) + '/experiment_html_charts'
    html_dir = out_dir + '/' + path_array[-1]
    os.system('mkdir ' + html_dir)

    #Constants
    staleness = 0
    machs = {}
    for dirpath, dirname, filenames in os.walk(path):
        machine_name = dirpath.split('/')[-1]
        if 'large' in machine_name:
            machs[machine_name] = []
            for file in filenames:

                machs[machine_name].append(int(file.split('_')[-1]))
            machs[machine_name] = list(set(machs[machine_name]))
            machs[machine_name].sort()
    print repr(machs)
    epochs = 0
    runs = 30

    for dirpath, dirname, filenames in os.walk(path):
        for file in filenames:
            file_path = os.path.join(dirpath, file)
            mach_name = dirpath.split('/')[-1]
            os.system('mkdir ' + html_dir + '/' + mach_name)
            if 'epoch' in file and file_path[-1] == str(machs[mach_name][0]):
                file_path = '_'.join(file_path.split('_')[:-1])
                matrices = []
                for mach in machs[mach_name]:
                    f = open(file_path + '_' + str(mach), 'r')
                    data = f.readlines()
                    matrix = []
                    meta_data = data[0].split()
                    epochs = meta_data[1]
                    staleness = meta_data[2]
                    for line in data[1:]:
                        arr = map(lambda x: float(x.strip())*get_dist_chunks(num_chunks, mach, used_chunks), line.split('\t'))
                        arr[0] = int(arr[0])
                        matrix.append(arr)

                    for j in range(1, len(matrix[0])):
                        total = matrix[0][j]
                        for i in range(1, len(matrix)):
                            matrix[i][j] = matrix[i][j] - total
                            total += matrix[i][j]
                    matrices.append(matrix)

                f2 = open('tmp', 'w')
                matrix_str = 'var arrays = [mach1'
                std_dev_str = 'stdDevs = [[]'
                mean_str = 'means = [[]'

                f2.write('var machine_name = "' + mach_name + '";\n')
                f2.write('var machines = ' + str(len(machs[mach_name])) + ';\n')
                f2.write('var epochs = ' + str(epochs) + ';\n')
                f2.write('var runs = ' + str(runs) + ';\n')
                f2.write('var staleness = ' + str(staleness) + ';\n')
                for i in range(len(machs[mach_name])):
                    f2.write('mach' + str(i + 1) + ' = ' + repr(matrices[i]))
                    f2.write('\n\n')
                    if i != 0:
                        matrix_str += ',mach' + str(i + 1)
                        std_dev_str += ',[]'
                        mean_str += ',[]'
                matrix_str += '];\n'
                std_dev_str += '];\n'
                mean_str += '];\n'
                f2.write(matrix_str)
                f2.write(std_dev_str)
                f2.write(mean_str)
                f2.close()
                os.system('cat html1 tmp html2_epoch > ' + html_dir + '/' + mach_name + '/' +  '_'.join(file.split('_')[:-2]) + '.html')

def create_speedup_epoch_html_files_from_processed_data(path, num_chunks, used_chunks, staleness_arr):

    path_array =  path.split('/')
    out_dir = '/'.join(path_array[:-2]) + '/experiment_html_charts'
    html_dir = out_dir + '/' + path_array[-1]
    os.system('mkdir ' + html_dir)

    #Constants
    staleness = 0
    machs = {}
    for dirpath, dirname, filenames in os.walk(path):
        machine_name = dirpath.split('/')[-1]
        if 'large' in machine_name:
            machs[machine_name] = []
            for file in filenames:

                machs[machine_name].append(int(file.split('_')[-1]))
            machs[machine_name] = list(set(machs[machine_name]))
            machs[machine_name].sort()
    print repr(machs)
    epochs = 0
    runs = 30



    for staleness in staleness_arr:
        matrices = []
        for mach_name in machs.keys():
            epochs = len(machs[mach_name])
            matrix = []
            for mach in machs[mach_name]:
                f = open(path + '/' + mach_name + '/epoch_times_' + mach_name + '_staleness_' + str(staleness) + '_machines_' + str(mach), 'r')
                data = f.readlines()
                meta_data = data[0].split()
                num_pochs = int(meta_data[1])
                epoch_mat = map(lambda x: map(lambda y: float(y.strip()), x.split('\t')[1:]), data[1:])
                arr = [sum(x)/float((num_pochs - 1))*get_dist_chunks(num_chunks, mach, used_chunks) for x in zip(*epoch_mat)]
                arr.insert(0, str(mach))
                matrix.append(arr)
            matrices.append(matrix)

        f2 = open('tmp', 'w')
        matrix_str = 'var arrays = [mach1'
        std_dev_str = 'stdDevs = [[]'
        mean_str = 'means = [[]'

        f2.write('var machine_names = ' + repr(machs.keys()) + ';\n')
        f2.write('var machines = ' + str(len(matrices)) + ';\n')
        f2.write('var epochs = ' + str(epochs) + ';\n')
        f2.write('var runs = ' + str(runs) + ';\n')
        f2.write('var staleness = ' + str(staleness) + ';\n')
        for i in range(len(matrices)):
            f2.write('mach' + str(i + 1) + ' = ' + repr(matrices[i]))
            f2.write('\n\n')
            if i != 0:
                matrix_str += ',mach' + str(i + 1)
                std_dev_str += ',[]'
                mean_str += ',[]'
        matrix_str += '];\n'
        std_dev_str += '];\n'
        mean_str += '];\n'
        f2.write(matrix_str)
        f2.write(std_dev_str)
        f2.write(mean_str)
        f2.close()
        os.system('cat html1 tmp html2_overhead > ' + html_dir + '/staleness_' + str(staleness) + '_speedups.html')



def create_overhead_html_files_from_processed_data(path, num_chunks, used_chunks):
    path_array =  path.split('/')
    out_dir = '/'.join(path_array[:-2]) + '/experiment_html_charts'
    html_dir = out_dir + '/' + path_array[-1]
    os.system('mkdir ' + html_dir)


    #Constants
    staleness = 0
    machs = {}
    for dirpath, dirname, filenames in os.walk(path):
        machine_name = dirpath.split('/')[-1]
        if 'large' in machine_name:
            machs[machine_name] = []
            for file in filenames:

                machs[machine_name].append(int(file.split('_')[-1]))
            machs[machine_name] = list(set(machs[machine_name]))
            machs[machine_name].sort()

    epochs = 1
    runs = 30

    for dirpath, dirname, filenames in os.walk(path):
        for file in filenames:
            file_path = os.path.join(dirpath, file)
            mach_name = dirpath.split('/')[-1]

            os.system('mkdir ' + html_dir + '/' + mach_name)
            if 'overhead' in file and file_path[-1] == str(machs[mach_name][0]):
                file_path = '_'.join(file_path.split('_')[:-1])
                matrices = []
                epochs = len(machs[mach_name])
                for mach in machs[mach_name]:
                    f = open(file_path + '_' + str(mach), 'r')
                    data = f.readlines()
                    matrix = []
                    meta_data = data[0].split()
                    staleness = meta_data[2]

                    arr = map(lambda x: float(x.strip())*get_dist_chunks(num_chunks, mach, used_chunks), data[1].split('\t'))
                    arr.insert(0, str(mach))
                    matrices.append(arr)

                matrices = [matrices]

                f2 = open('tmp', 'w')
                matrix_str = 'var arrays = [mach1'
                std_dev_str = 'stdDevs = [[]'
                mean_str = 'means = [[]'

                f2.write('var machine_names = "";\n')
                f2.write('var machines = ' + str(1) + ';\n')
                f2.write('var epochs = ' + str(epochs) + ';\n')
                f2.write('var runs = ' + str(runs) + ';\n')
                f2.write('var staleness = ' + str(staleness) + ';\n')
                for i in range(1):
                    f2.write('mach' + str(i + 1) + ' = ' + repr(matrices[i]))
                    f2.write('\n\n')
                    if i != 0:
                        matrix_str += ',mach' + str(i + 1)
                        std_dev_str += ',[]'
                        mean_str += ',[]'
                matrix_str += '];\n'
                std_dev_str += '];\n'
                mean_str += '];\n'
                f2.write(matrix_str)
                f2.write(std_dev_str)
                f2.write(mean_str)
                f2.close()
                os.system('cat html1 tmp html2_overhead > ' + html_dir + '/' + mach_name + '/' +  '_'.join(file.split('_')[:-2]) + '.html')



def get_dist_chunks(num_chunks, num_machs, used_chunks):
    if num_chunks == 1:
        return 1
    else:
        return math.ceil(float(num_chunks) / num_machs) / math.ceil(float(used_chunks) / num_machs)

def get_num_chunks(data_set, projected):
    if projected == 1 :
        if data_set == 'covtype':
            return 408
        elif data_set == 'mnist8m':
            return 119
        else:
            return 1
    else:
        return 1


def main():
    clp = sys.argv
    data_set = clp[1]
    run = clp[2]
    try:
        projected = int(clp[3])
    except:
        projectd = 0

    used_chunks = 16

    num_chunks = get_num_chunks(data_set, projected)

    #Set this
    staleness = [0]

    path_to_walk = '/Users/Kevin/MLSchedule/experiment_data/' + data_set + '/' + run
    out_file = '/Users/Kevin/MLSchedule/experiment_data_processing_scripts/processed_experiments/epoch-times-' + run
    os.system('mkdir ' + out_file)
    #PROCESS FILES
    walk_directories_to_process(path_to_walk, out_file, staleness)
    create_epoch_html_files_from_processed_data(out_file, num_chunks, used_chunks)
    create_speedup_epoch_html_files_from_processed_data(out_file, num_chunks, used_chunks, staleness)
    create_overhead_html_files_from_processed_data(out_file, num_chunks, used_chunks)


    #GRAPH FILES
    #walk_directories_to_graph(path_to_walk)

if __name__ == "__main__":
    main()

