import sys
import math


cla = sys.argv
path = cla[1]
which_log = cla[2]

f = open(path + '/logs/log_trial_' + which_log, 'r')
f2 = open(path + '/log_metrics_file', 'a')
f3 = open(path + '/epochs', 'a')
f4 = open(path + '/epoch_outliers', 'a')
f5 = open(path + '/stats', 'a')

time_array = []
for line in f:
    if ': Job' in line:
        time_array.append(float(line.split()[-2]))


times_array = [time_array[0], time_array[1], time_array[2], sum(time_array[3:])]

f2.write(repr(times_array) + '\n')
f3.write(repr(time_array[3:]) + '\n')
mean = sum(time_array[3:])/len(time_array[3:])
std_dev = math.sqrt(sum(map(lambda x: (x-mean)**2, time_array[3:]))/len(time_array[3:]))
outliers = []
for i, time in enumerate(time_array[3:]):
    if time > mean + 3*std_dev:
        outliers.append([i + 1, time])
f4.write(repr(outliers) + '\n')
sorted_time_array = sorted(enumerate(time_array[3:]),reverse=True, key=lambda x: x[1])
f5.write(repr([mean, std_dev, sorted_time_array[:10]]) + '\n')
