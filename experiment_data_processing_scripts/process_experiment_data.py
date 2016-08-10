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
        ml_time = float(data[idx + 7].split()[-1])
        return epoch_time, ml_time
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

def create_overhead_file(path):
    f = open(path, 'w')
    f.write('Machines\nSpin Up Time\nFile Creation Time\nData Fetch Time\n')
    f.close()

def write_overhead_column_times(path, spin_up_time, file_creation_time, data_fetch_time, num_machs):
    f = open(path, 'r')
    data = f.readlines()
    f.close()
    f = open(path, 'w')
    f.write(data[0].strip() + '\t' + str(num_machs) + '\n')
    f.write(data[1].strip() + '\t' + str(spin_up_time) + '\n')
    f.write(data[2].strip() + '\t' + str(file_creation_time) + '\n')
    f.write(data[3].strip() + '\t' + str(data_fetch_time) + '\n')
    f.close()

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

def read_spin_up_and_file_creation(file_path):
    f = open(file_path, 'r')
    data = f.readlines()
    delimit = '********************************************************\n'
    idx = -1
    if delimit in data:
        idx = data.index(delimit)
    else:
        print 'WE HAD AN ERROR DURING THE EXPERIMENT, RETRAIN'
    if idx > -1:
        spin_up_time = float(data[idx + 4].split()[-1])
        file_creation_time = float(data[idx + 5].split()[-1])
        return spin_up_time, file_creation_time
    else:
        sys.exit('AN ERROR OCCURRED WHHILE TRYING TO READ THE EPOCH TIME')
        return -1

def read_data_stats(file_path):
    f = open(file_path, 'r')
    data = f.readlines()
    delimit = '********************************************************\n'
    idx = -1
    if delimit in data:
        idx = data.index(delimit)
    else:
        print 'WE HAD AN ERROR DURING THE EXPERIMENT, RETRAIN'
    if idx > -1:
        features = int(data[1].split()[-1])
        labels = int(data[2].split()[-1])
        train_data_size = int(data[3].split()[-1])

        return features, labels, train_data_size
    else:
        sys.exit('AN ERROR OCCURRED WHHILE TRYING TO READ THE EPOCH TIME')
        return -1

def read_data_upload_times(file_path):
    f = open(file_path, 'r')
    data = f.readlines()
    delimit = '********************************************************\n'
    idx = -1
    if delimit in data:
        idx = data.index(delimit)
    else:
        print 'WE HAD AN ERROR DURING THE EXPERIMENT, RETRAIN'
    if idx > -1:
        data_fetch_time = float(data[idx + 6].split()[-1])
        return data_fetch_time
    else:
        sys.exit('AN ERROR OCCURRED WHHILE TRYING TO READ THE EPOCH TIME')
        return -1

def parse_run_out_file(path, out_file):
    mach_name = path.split('/')[-1].split('_')[0]
    split_path = path.split('_')

    num_sub_epochs = get_num_files('_'.join(split_path[:-1]))
    num_epochs = get_num_files('_'.join(split_path[:-3])) / num_sub_epochs
    num_runs = get_num_files('_'.join(split_path[:-5])) / (num_sub_epochs * num_epochs)
    num_machine_configs = get_num_files('_'.join(split_path[:-7])) / (num_sub_epochs * num_epochs * num_runs)
    print num_sub_epochs, num_epochs, num_runs, num_machine_configs

    path_prefix = '_'.join(split_path[:-7])
    create_mach_file(out_file + '/epoch_times_' + mach_name, num_epochs)
    create_mach_file(out_file + '/ml_overhead_' + mach_name, num_epochs)
    create_overhead_file(out_file + '/other_overhead_' + mach_name)

    spin_up_time = 0
    file_creation_time = 0
    data_fetch_time = 0
    features = 0
    labels = 0
    train_data_size = 0

    for mach_conf in range(num_machine_configs):
        num_machs = 2 ** mach_conf
        run_array = []
        machine_learning_overhead = []

        for run in range(num_runs):
            epoch_times = []
            epoch_overhead_times = []
            for epoch in range(num_epochs):
                sub_epoch_arr = []
                sub_epoch_overhead = []
                for sub_epoch in range(num_sub_epochs):
                    # try:
                    file_path = create_file_path(path_prefix, num_machs, run, epoch, sub_epoch)
                    sub_epoch_time, ml_time = read_epoch_time(file_path)
                    sub_epoch_arr.append(sub_epoch_time)
                    sub_epoch_overhead.append(ml_time - sub_epoch_time)
                    if run == 0 and epoch == 0 and sub_epoch == 0:
                        spin_up_time, file_creation_time = read_spin_up_and_file_creation(file_path)
                        if mach_conf == 0:
                            features, labels, train_data_size = read_data_stats(file_path)
                    if run == 0 and epoch == 0:
                        data_fetch_time += read_data_upload_times(file_path)
                    
                    # except:
                    #     print file_path, ' DOES NOT EXIST'
                epoch_times.append(sum(sub_epoch_arr))
                epoch_overhead_times.append(sum(sub_epoch_overhead))
            run_array.append(epoch_times)
            machine_learning_overhead.append(epoch_overhead_times)
        mach_epoch_times = [float(sum(x))/num_runs for x in zip(*run_array)]
        mach_ml_overhead = [float(sum(x))/num_runs for x in zip(*machine_learning_overhead)]
        write_mach_column_to_file(out_file + '/epoch_times_' + mach_name, mach_epoch_times, num_machs)
        write_mach_column_to_file(out_file + '/ml_overhead_' + mach_name, mach_ml_overhead, num_machs)
        write_overhead_column_times(out_file + '/other_overhead_' + mach_name, spin_up_time, file_creation_time, data_fetch_time, num_machs)

    write_data_stats(out_file + '/data_stats', features, labels, train_data_size)



def create_html_files_from_processed_data(path):
    path_array =  path.split('/')
    out_dir = '/'.join(path_array[:-2]) + '/experiment_html_charts'
    html_dir = out_dir + '/' + path_array[-1]
    os.system('mkdir ' + html_dir)

    for dirpath, dirname, filenames in os.walk(path):
        print filenames
        for file in filenames:
            if 'epoch_times' in file:
                mach_name = file.split('_')[-1]
                file_path = os.path.join(dirpath, file)
                f2 = open(file_path, 'r')
                data = f2.readlines()

                f = open('tmp', 'w')
                f.write('function drawChart() {\n')
                f.write('var data = google.visualization.arrayToDataTable(\n[')

                for (i,line) in enumerate(data):
                    line = line.strip()
                    if line != '':
                        arr = line.split('\t')
                        if i != 0:
                            arr = [arr[0]] + map(lambda x: float(x), arr[1:])
                        f.write(repr(arr) + ',\n')
                f.write(']);\n\n')
                f.write('var options = {\n')
                f.write('title: "Epoch Durations ' + mach_name  + '",\n')
                f.write('curveType: "function",\n')
                f.write('legend: { position: "bottom" }\n};\n')
                f.write('var chart = new google.visualization.LineChart(document.getElementById("curve_chart1"));\n')
                f.write('chart.draw(data, options);\n}')
                f.close()
                file_path_2 = os.path.join(dirpath, 'ml_overhead_' + mach_name)
                f3 = open(file_path_2, 'r')
                data2 = f3.readlines()

                f3 = open('tmp2', 'w')
                f3.write('\n\n')
                f3.write('function drawChart2() {\n')
                f3.write('var data = google.visualization.arrayToDataTable(\n[')

                for (i,line) in enumerate(data2):
                    line = line.strip()
                    if line != '':
                        arr = line.split('\t')
                        if i != 0:
                            arr = [arr[0]] + map(lambda x: float(x), arr[1:])
                        f3.write(repr(arr) + ',\n')
                f3.write(']);\n\n')
                f3.write('var options = {\n')
                f3.write('title: "Machine Learning Algorithm Overhead ' + mach_name  + '",\n')
                f3.write('curveType: "function",\n')
                f3.write('legend: { position: "bottom" }\n};\n')
                f3.write('var chart = new google.visualization.LineChart(document.getElementById("curve_chart2"));\n')
                f3.write('chart.draw(data, options);\n}')

                f3.close()
                file_path_3 = os.path.join(dirpath, 'other_overhead_' + mach_name)
                f4 = open(file_path_3, 'r')
                data3 = f4.readlines()

                f4 = open('tmp3', 'w')
                f4.write('\n\n')
                f4.write('function drawChart3() {\n')
                f4.write('var data = google.visualization.arrayToDataTable(\n[')

                for (i,line) in enumerate(data3):
                    line = line.strip()
                    if line != '':
                        arr = line.split('\t')
                        if i != 0:
                            arr = [arr[0]] + map(lambda x: float(x), arr[1:])
                        f4.write(repr(arr) + ',\n')
                f4.write(']);\n\n')
                f4.write('var options = {\n')
                f4.write('chart: {\n')
                f4.write('title: "Other Overheads ' + mach_name  + '",\n')
                f4.write('},\nbars: "horizontal",\n')
                f4.write('legend: { position: "bottom" }\n};\n')
                f4.write('var chart = new google.charts.Bar(document.getElementById("barchart_material"));\n')
                f4.write('chart.draw(data, options);\n}')
                f4.close()

                file_path_4 = os.path.join(dirpath, 'data_stats')
                f5 = open(file_path_4, 'r')
                data4 = f5.readlines()[0].split('\t')
                f5 = open('tmp4', 'w')
                f5.write('\n\n')
                f5.write('</script>\n</head>\n<body>\n')
                f5.write('<h1>Data Stats</h1>\n')
                f5.write('<h3>Dataset Name: ' + path_array[-1].split('-')[2] + '</h3>\n')
                f5.write('<h3>Number of Features: ' + data4[0].strip() + '</h3>\n')
                f5.write('<h3>Number of Labels: ' + data4[1].strip() + '</h3>\n')
                f5.write('<h3>Training Set Size: ' + data4[2].strip() + '</h3>\n')
                f5.write('<div id="curve_chart1" style="width: 100%; height: 500px;"></div>\n')
                f5.write('<div id="curve_chart2" style="width: 100%; height: 500px;"></div>\n')
                f5.write('<div id="barchart_material" style="width: 100%; height: 500px;"></div>\n')
                f5.write('</body>\n')
                f5.write('</html>\n')
                f5.close()




                os.system('cat html1 tmp tmp2 tmp3 tmp4 > ' + html_dir + '/' + mach_name + '.html')

                os.system('rm tmp')
                os.system('rm tmp2')
                os.system('rm tmp3')
                os.system('rm tmp4')

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

