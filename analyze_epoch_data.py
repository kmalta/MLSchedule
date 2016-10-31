import sys
import ast
import math
import os

clp = sys.argv
try:
    if clp[1] == 'all'
    for directory in os.walk('aristotle_all_in_one_exps'):
        directories = directory[1]
        break
except:
    directories == ''


for directory in directories:

    path = 'aristotle_all_in_one_exps/' + directory
    epochs = 500

    f = open(path + '/epochs', 'r')
    line_count = len(f.readlines())
    f.close()
    if line_count < 30:
        continue

    f = open(path + '/epochs', 'r')
    f2 = open(path + '/epoch_averages', 'w')
    f3 = open(path + '/epoch_stddevs', 'w')
    f4 = open(path + '/averages_based_on_epochs', 'w')

    def mean(array):
        return float(sum(array))/len(array)

    def stddev(array):
        return math.sqrt(sum(map(lambda x: (x-mean(array))**2, array))/len(array))



    epoch_lists = []
    epoch_averages = []
    epoch_stddevs = []
    epoch_init_range_averages = []
    epoch_init_range_stddevs = []
    epoch_sub_range_averages = []
    epoch_sub_range_stddevs = []
    for line in f:
        epoch_list = ast.literal_eval(line)
        epoch_lists.append(epoch_list)
        epoch_averages.append(mean(epoch_list))
        epoch_stddevs.append(stddev(epoch_list))
        epoch_init_range_averages.append(mean(epoch_list[:30]))
        epoch_init_range_stddevs.append(stddev(epoch_list[:30]))
        epoch_sub_range_averages.append(mean(epoch_list[30:]))
        epoch_sub_range_stddevs.append(stddev(epoch_list[30:]))


    column_averages = []
    column_stddevs = []
    trials = len(epoch_lists)
    for i in range(len(epoch_lists[0])):
        column = [trial[i] for trial in epoch_lists]
        column_mean = mean(column)
        column_stddev = stddev(column)
        column_averages.append(column_mean)
        column_stddevs.append(column_stddev)

    for i in range(len(column_averages)):
        f2.write(str(column_averages[i]) + '\n')
        f3.write(str(column_stddevs[i]) + '\n')

    f4.write(repr(epoch_averages) + '\n')
    f4.write(repr(epoch_init_range_averages) + '\n')
    f4.write(repr(epoch_sub_range_averages) + '\n')
    f4.write(repr(epoch_stddevs) + '\n')
    f4.write(repr(epoch_init_range_stddevs) + '\n')
    f4.write(repr(epoch_sub_range_stddevs) + '\n')
    f4.write(repr(mean(epoch_averages)) + '\n')
    f4.write(repr(mean(epoch_init_range_averages)) + '\n')
    f4.write(repr(mean(epoch_sub_range_averages)) + '\n')
    f4.write(repr(mean(epoch_stddevs)) + '\n')
    f4.write(repr(mean(epoch_init_range_stddevs)) + '\n')
    f4.write(repr(mean(epoch_sub_range_stddevs)) + '\n')



